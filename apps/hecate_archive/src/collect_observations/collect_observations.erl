%%% @doc The mesh subscriber: hear an observation, check it, put it on the tape.
%%%
%%% Sensors publish verbatim observations on one topic. This hears them, and for
%%% each one:
%%%
%%%   1. checks the envelope carries what makes a record citable (stream, seq,
%%%      payload, hash, timestamps),
%%%   2. verifies `payload_sha256' against the payload actually received,
%%%   3. drops it if this exact `{source, dataset, seq}' is already on the tape,
%%%   4. appends it, and only then
%%%   5. tells the gap ledger the seq arrived.
%%%
%%% Order matters: the ledger is told AFTER the append succeeds, so a write
%%% failure leaves a hole the ledger will notice rather than a hole it has
%%% already been told to expect.
%%%
%%% Dedupe is a bounded window over `{source, dataset, seq}', because delivery is
%%% at-least-once and a reconnect re-delivers. A duplicate seq carrying a
%%% DIFFERENT payload hash is not a duplicate, it is a contradiction, and it is
%%% logged as an error rather than silently resolved either way.
%%%
%%% Re-subscribes on teardown. Degrades safely while the mesh is dark.
-module(collect_observations).
-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(DEFAULT_TOPIC, <<"archive/observations">>).
-define(DEFAULT_MAX_SEEN, 20000).
-define(RESUB_MS, 5000).

-record(st, {topic    :: binary(),
             ref      :: reference() | undefined,
             max_seen :: pos_integer(),
             seen  = #{} :: #{term() => binary()},
             order = []  :: [term()]}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    Topic = topic(),
    logger:info("[archive] collecting observations on ~ts", [Topic]),
    self() ! subscribe,
    {ok, #st{topic = Topic, max_seen = max_seen()}}.

handle_call(_Req, _From, St) -> {reply, {error, unknown_call}, St}.
handle_cast(_Msg, St)        -> {noreply, St}.

handle_info(subscribe, St) ->
    {noreply, do_subscribe(St)};
handle_info({macula_event, _Ref, _Topic, Fact, _Meta}, St) ->
    {noreply, on_observation(Fact, St)};
handle_info({macula_event_gone, _Ref, _Reason}, St) ->
    self() ! subscribe,
    {noreply, St#st{ref = undefined}};
handle_info(_Info, St) ->
    {noreply, St}.

terminate(_Reason, _St) -> ok.

%% --- subscription ---

%% The lookup is caught, not just its result: the mesh subsystem may be absent
%% entirely (restarting, or not yet up), and a crash here would take the whole
%% archive down under `rest_for_one' along with the tape it supervises.
do_subscribe(St) ->
    subscribed(catch {hecate_om:macula_client(), hecate_om_identity:realm()}, St).

subscribed({{ok, Pool}, {ok, Realm}}, St) ->
    retry_unless_subscribed(sub(Pool, Realm, St#st.topic), St);
subscribed(_DarkOrNoRealm, St) ->
    erlang:send_after(?RESUB_MS, self(), subscribe),
    St.

%% A subscribe that threw (the pool's call timing out at boot, say) leaves the
%% ref undefined. Without re-arming, the archive stays silently deaf and every
%% fact published in the meantime is lost with nothing to show it.
retry_unless_subscribed(undefined, St) ->
    erlang:send_after(?RESUB_MS, self(), subscribe),
    St#st{ref = undefined};
retry_unless_subscribed(Ref, St) ->
    St#st{ref = Ref}.

sub(Pool, Realm, Topic) ->
    ref_of(catch macula:subscribe(Pool, Realm, Topic, self())).

ref_of({ok, Ref}) -> Ref;
ref_of(_Failed)   -> undefined.

%% --- ingest ---

on_observation(Fact, St) when is_map(Fact) ->
    checked(stream_key(Fact), Fact, St);
on_observation(_NotAMap, St) ->
    logger:warning("[archive] dropped: observation is not a map"),
    St.

%% An observation we cannot key is an observation we can neither dedupe nor
%% gap-track, so it is dropped rather than written where it would look like a
%% record but not behave like one.
checked(undefined, _Fact, St) ->
    logger:warning("[archive] dropped: observation missing source/dataset/epoch/seq"),
    St;
checked(Key, Fact, St) ->
    verified(payload_ok(Fact), Key, Fact, St).

verified(false, Key, _Fact, St) ->
    logger:error("[archive] dropped ~p: payload_sha256 does not match payload", [Key]),
    St;
verified(true, Key, Fact, St) ->
    fresh(maps:get(Key, St#st.seen, undefined), Key, Fact, St).

fresh(undefined, Key, Fact, St) ->
    store(Key, Fact, St);
fresh(Sha, Key, Fact, St) ->
    duplicate(Sha =:= mget(payload_sha256, Fact), Key, St).

%% Same seq, same bytes: a redelivery, which is expected and cheap to ignore.
duplicate(true, _Key, St) ->
    St;
%% Same seq, different bytes: two different records claim the same position in a
%% stream. One of them is wrong and we cannot know which, so neither is quietly
%% preferred.
duplicate(false, Key, St) ->
    logger:error("[archive] CONTRADICTION at ~p: seq re-used with a different payload", [Key]),
    St.

store({Source, Dataset, _Epoch, _Seq} = Key, Fact, St) ->
    appended(seal_segments:append(Source, Dataset, macula_cbor_nif:pack(Fact)),
             Key, mget(payload_sha256, Fact), St).

appended(ok, {Source, Dataset, Epoch, Seq} = Key, Sha, St) ->
    report_gaps:observe(Source, Dataset, Epoch, Seq),
    remember(Key, Sha, St);
%% The tape refused it. Do NOT tell the ledger, and do NOT remember it: the fact
%% is lost, and the next seq to arrive must show up as the gap it is.
appended({error, Reason}, Key, _Sha, St) ->
    logger:error("[archive] LOST ~p: tape rejected the record: ~p", [Key, Reason]),
    St.

%% --- bounded dedupe window ---

remember(Key, Sha, #st{seen = Seen, order = Order} = St) ->
    evict(St#st{seen = maps:put(Key, Sha, Seen), order = [Key | Order]}).

evict(#st{order = Order, max_seen = Max} = St) when length(Order) =< Max ->
    St;
evict(#st{seen = Seen, order = Order, max_seen = Max} = St) ->
    {Keep, Drop} = lists:split(Max, Order),
    St#st{seen = lists:foldl(fun maps:remove/2, Seen, Drop), order = Keep}.

%% --- envelope ---

%% `epoch' is part of the key, not decoration. A sensor holds no store, so its
%% sequence counter restarts from zero when it does; scoping the sequence to a
%% run is what lets the archive tell "the sensor restarted" (about which it
%% claims nothing) from "facts went missing" (which it counts).
stream_key(Fact) ->
    key(mget(source, Fact), mget(dataset, Fact), mget(epoch, Fact), mget(seq, Fact)).

key(Source, Dataset, Epoch, Seq)
  when is_binary(Source), Source =/= <<>>,
       is_binary(Dataset), Dataset =/= <<>>,
       is_integer(Epoch), Epoch > 0,
       is_integer(Seq), Seq >= 0 ->
    {Source, Dataset, Epoch, Seq};
key(_Source, _Dataset, _Epoch, _Seq) ->
    undefined.

%% The hash is the sensor's claim about what it saw. Checking it here is what
%% makes the tape's contents attributable to the upstream response rather than to
%% whatever survived the wire.
payload_ok(Fact) ->
    hash_matches(mget(payload, Fact), mget(payload_sha256, Fact)).

hash_matches(Payload, Sha) when is_binary(Payload), is_binary(Sha) ->
    binary:encode_hex(crypto:hash(sha256, Payload), lowercase) =:= lower(Sha);
hash_matches(_Payload, _Sha) ->
    false.

lower(Bin) -> list_to_binary(string:lowercase(binary_to_list(Bin))).

%% Facts cross the wire as CBOR, and a key may arrive as an atom or as a binary
%% depending on how the sensor built it. Accept either; store what arrived.
mget(K, M) ->
    maps:get(K, M, maps:get(atom_to_binary(K, utf8), M, undefined)).

%% --- config ---

topic() ->
    bin(os:getenv("HECATE_ARCHIVE_TOPIC"),
        application:get_env(hecate_archive, topic, ?DEFAULT_TOPIC)).

bin(S, _Fallback) when is_list(S), S =/= "" -> unicode:characters_to_binary(S);
bin(_Unset, Fallback)                       -> Fallback.

max_seen() ->
    parse_int(os:getenv("HECATE_ARCHIVE_MAX_SEEN"),
              application:get_env(hecate_archive, max_seen, ?DEFAULT_MAX_SEEN)).

parse_int(S, Fallback) when is_list(S), S =/= "" ->
    to_int(string:to_integer(S), Fallback);
parse_int(_Unset, Fallback) ->
    Fallback.

to_int({I, _Rest}, _Fallback) when is_integer(I), I > 0 -> I;
to_int(_NotInt, Fallback)                               -> Fallback.
