%%% @doc The tape: framed append, timed roll, seal with a checksum.
%%%
%%% One open segment per stream (a stream is a `{source, dataset}' pair). Records
%%% are length-prefixed CBOR:
%%%
%%%     <<Len:32/big, Cbor:Len/binary>>
%%%
%%% so a truncated tail from an unclean shutdown costs ONE record, not a segment.
%%%
%%% Segments are bucketed by ARRIVAL wall-clock at the archive, never by a
%%% record's own timestamps. That is deliberate: a late-arriving observation
%%% would otherwise target an already-sealed segment, and a sealed segment that
%%% can be reopened is not sealed. Nothing is lost by this — every record carries
%%% its own `request_at' and `response_at' inside, so segment boundaries are a
%%% storage detail and never a temporal claim.
%%%
%%% On seal: the segment is gzipped, its SHA-256 is written beside it, and the
%%% plain file is removed. The checksum is over the UNCOMPRESSED record stream,
%%% computed incrementally as records land, so it stays meaningful if the
%%% compression ever changes and costs no re-read.
%%%
%%% A segment with no `.sha256' beside it is an UNCLEAN segment. It is flagged,
%%% never repaired: a tape that quietly patches itself is a tape you cannot cite.
-module(seal_segments).
-behaviour(gen_server).

-export([start_link/0, append/3, health/0, root/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(DEFAULT_ROOT, "/bulk0/hecate-archive").
-define(DEFAULT_ROLL_MS, 3600000).      %% hourly
-define(HEALTH_TIMEOUT, 2000).
-define(APPEND_TIMEOUT, 15000).

-record(seg, {path       :: file:filename_all(),
              fd         :: file:io_device(),
              ctx        :: crypto:hash_state(),
              bucket     :: integer(),     %% ms, start of the arrival bucket
              records = 0 :: non_neg_integer(),
              bytes   = 0 :: non_neg_integer()}).

-record(st, {root       :: string(),
             roll_ms    :: pos_integer(),
             segs = #{} :: #{{binary(), binary()} => #seg{}},
             last_error = none :: none | term()}).

%% @doc Append one already-encoded CBOR record to a stream's open segment.
%% Synchronous on purpose: the caller must not mark a fact seen until the tape
%% has taken it.
-spec append(binary(), binary(), binary()) -> ok | {error, term()}.
append(Source, Dataset, Cbor) ->
    gen_server:call(?MODULE, {append, Source, Dataset, Cbor}, ?APPEND_TIMEOUT).

%% @doc Tape health. Degraded means the last write failed, which means facts
%% arriving now are being lost.
-spec health() -> hecate_om_service:health().
health() ->
    reply_health(catch gen_server:call(?MODULE, health, ?HEALTH_TIMEOUT)).

%% @doc The archive root. Also read by report_gaps for its own ledger.
-spec root() -> string().
root() ->
    env_str("HECATE_ARCHIVE_ROOT", hecate_archive, root, ?DEFAULT_ROOT).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    process_flag(trap_exit, true),
    Root = root(),
    logger:info("[archive] tape at ~ts, roll ~bs", [Root, roll_ms() div 1000]),
    {ok, #st{root = Root, roll_ms = roll_ms()}}.

handle_call({append, Source, Dataset, Cbor}, _From, St) ->
    {Reply, St2} = do_append({Source, Dataset}, Cbor, St),
    {reply, Reply, St2};
handle_call(health, _From, #st{last_error = none} = St) ->
    {reply, ok, St};
handle_call(health, _From, #st{last_error = E} = St) ->
    {reply, {down, E}, St};
handle_call(_Req, _From, St) ->
    {reply, {error, unknown_call}, St}.

handle_cast(_Msg, St) -> {noreply, St}.
handle_info(_Info, St) -> {noreply, St}.

%% Seal everything on the way out. An unsealed segment is an unclean segment, and
%% a clean shutdown is the one time we can always avoid producing one.
terminate(_Reason, #st{segs = Segs} = St) ->
    _ = maps:fold(fun(Stream, Seg, _) -> seal(Stream, Seg, St) end, ok, Segs),
    ok.

%% --- append ---

do_append(Stream, Cbor, St) ->
    Bucket = bucket(erlang:system_time(millisecond), St#st.roll_ms),
    write_to(segment_for(Stream, Bucket, St), Stream, Cbor, St).

%% Roll first (seal the old, open the new), then write. Both the "no segment yet"
%% and the "bucket moved on" cases land here as `{ok, Seg, St}' or an error that
%% aborts the write.
segment_for(Stream, Bucket, St) ->
    current(maps:get(Stream, St#st.segs, undefined), Stream, Bucket, St).

current(#seg{bucket = B} = Seg, _Stream, B, St) ->
    {ok, Seg, St};
current(undefined, Stream, Bucket, St) ->
    open(Stream, Bucket, St);
current(Old, Stream, Bucket, St) ->
    _ = seal(Stream, Old, St),
    open(Stream, Bucket, St#st{segs = maps:remove(Stream, St#st.segs)}).

write_to({error, Reason}, _Stream, _Cbor, St) ->
    {{error, Reason}, St#st{last_error = Reason}};
write_to({ok, Seg, St}, Stream, Cbor, _St0) ->
    Frame = <<(byte_size(Cbor)):32/big, Cbor/binary>>,
    wrote(file:write(Seg#seg.fd, Frame), Stream, Seg, Frame, St).

wrote(ok, Stream, Seg, Frame, St) ->
    Seg2 = Seg#seg{ctx     = crypto:hash_update(Seg#seg.ctx, Frame),
                   records = Seg#seg.records + 1,
                   bytes   = Seg#seg.bytes + byte_size(Frame)},
    {ok, St#st{segs = maps:put(Stream, Seg2, St#st.segs), last_error = none}};
wrote({error, Reason}, _Stream, _Seg, _Frame, St) ->
    logger:error("[archive] tape write failed: ~p", [Reason]),
    {{error, Reason}, St#st{last_error = Reason}}.

%% --- open / seal ---

open(Stream, Bucket, St) ->
    Path = segment_path(St#st.root, Stream, Bucket),
    ok = filelib:ensure_dir(Path),
    opened(file:open(Path, [raw, write, binary]), Path, Stream, Bucket, St).

opened({ok, Fd}, Path, Stream, Bucket, St) ->
    Seg = #seg{path = Path, fd = Fd, ctx = crypto:hash_init(sha256), bucket = Bucket},
    logger:info("[archive] segment open: ~ts", [Path]),
    {ok, Seg, St#st{segs = maps:put(Stream, Seg, St#st.segs)}};
opened({error, Reason}, Path, _Stream, _Bucket, _St) ->
    logger:error("[archive] cannot open segment ~ts: ~p", [Path, Reason]),
    {error, Reason}.

%% An EMPTY segment is removed rather than sealed: an hour in which a stream said
%% nothing should leave no artefact, so the presence of a segment always means
%% records arrived. Absence is the gap ledger's business, not the tape's.
seal({Source, Dataset}, #seg{records = 0, path = Path, fd = Fd}, _St) ->
    _ = file:close(Fd),
    _ = file:delete(Path),
    logger:debug("[archive] empty segment dropped: ~ts/~ts", [Source, Dataset]),
    ok;
seal({Source, Dataset}, Seg, _St) ->
    _ = file:close(Seg#seg.fd),
    Sha = binary:encode_hex(crypto:hash_final(Seg#seg.ctx), lowercase),
    _ = compress(Seg#seg.path),
    ok = write_checksum(Seg#seg.path, Sha),
    logger:info("[archive] sealed ~ts (~b records, ~b bytes)",
                [Seg#seg.path, Seg#seg.records, Seg#seg.bytes]),
    hecate_archive_facts:sealed(#{source  => Source,
                                  dataset => Dataset,
                                  segment => list_to_binary(filename:basename(Seg#seg.path)),
                                  records => Seg#seg.records,
                                  bytes   => Seg#seg.bytes,
                                  sha256  => Sha}).

%% gzip rather than zstd: OTP ships zlib, and an archive that needs an extra
%% native dependency to be readable is an archive with a dependency on its own
%% future build environment.
compress(Path) ->
    compressed(file:read_file(Path), Path).

compressed({ok, Bin}, Path) ->
    ok = file:write_file(Path ++ ".gz", zlib:gzip(Bin)),
    file:delete(Path);
compressed({error, Reason}, Path) ->
    logger:error("[archive] cannot compress ~ts: ~p (left uncompressed)", [Path, Reason]),
    ok.

%% sha256sum(1) format, naming the UNCOMPRESSED segment: `sha256sum -c` after a
%% gunzip verifies the tape with no bespoke tooling.
write_checksum(Path, Sha) ->
    Line = [Sha, "  ", filename:basename(Path), "\n"],
    file:write_file(checksum_path(Path), Line).

checksum_path(Path) ->
    filename:rootname(Path) ++ ".sha256".

%% --- paths ---

%% <root>/<source>/<dataset>/<YYYY>/<MM>/<DD>/<source>-<YYYYMMDD>T<HH><MM>.cbor
segment_path(Root, {Source, Dataset}, Bucket) ->
    {{Y, M, D}, {H, Min, _S}} =
        calendar:system_time_to_universal_time(Bucket, millisecond),
    Name = io_lib:format("~ts-~4..0b~2..0b~2..0bT~2..0b~2..0b.cbor",
                         [Source, Y, M, D, H, Min]),
    filename:join([Root, safe(Source), safe(Dataset),
                   io_lib:format("~4..0b", [Y]),
                   io_lib:format("~2..0b", [M]),
                   io_lib:format("~2..0b", [D]),
                   lists:flatten(Name)]).

%% A source id becomes a directory name, so it may not travel outside the root.
safe(Bin) ->
    binary_to_list(binary:replace(Bin, [<<"/">>, <<"..">>, <<0>>], <<"_">>, [global])).

bucket(Now, RollMs) -> (Now div RollMs) * RollMs.

%% --- config ---

roll_ms() ->
    env_int("HECATE_ARCHIVE_ROLL_MS", hecate_archive, roll_ms, ?DEFAULT_ROLL_MS).

env_str(EnvVar, App, Key, Default) ->
    str(os:getenv(EnvVar), application:get_env(App, Key, Default)).

str(S, _Fallback) when is_list(S), S =/= "" -> S;
str(_Unset, Fallback)                       -> Fallback.

env_int(EnvVar, App, Key, Default) ->
    parse_int(os:getenv(EnvVar), application:get_env(App, Key, Default)).

parse_int(S, Fallback) when is_list(S), S =/= "" ->
    to_int(string:to_integer(S), Fallback);
parse_int(_Unset, Fallback) ->
    Fallback.

to_int({I, _Rest}, _Fallback) when is_integer(I), I > 0 -> I;
to_int(_NotInt, Fallback)                               -> Fallback.

%% A tape whose keeper is not answering is a tape that is not taking writes.
reply_health(ok)              -> ok;
reply_health({down, _} = Down) -> Down;
reply_health(Other)           -> {down, {tape_unreachable, Other}}.
