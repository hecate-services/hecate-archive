%%% @doc The ingest path, end to end, without a mesh.
%%%
%%% Drives `collect_observations' with facts shaped exactly as hecate-grid builds
%%% them, and checks the things that would make the tape lie: a record that does
%%% not match its own hash, a redelivery, a contradiction, a hole, and a sensor
%%% restart.
-module(collect_observations_tests).

-include_lib("eunit/include/eunit.hrl").

-define(SOURCE, <<"elia">>).
-define(DATASET, <<"ods161">>).
-define(EPOCH, 1753303400000).

ingest_test_() ->
    {setup, fun setup/0, fun cleanup/1,
     fun(Root) ->
         [{"a good observation lands and decodes back unchanged",
           fun() -> lands(Root) end},
          {"a payload that does not match its hash is dropped",
           fun() -> bad_hash(Root) end},
          {"a redelivery is written once", fun() -> redelivery(Root) end},
          {"a skipped seq is recorded as a gap", fun() -> gap(Root) end},
          {"a new epoch is a restart, not a gap", fun() -> restart(Root) end}]
     end}.

setup() ->
    Root = filename:join(["/tmp", "hecate-archive-ingest",
                          integer_to_list(erlang:unique_integer([positive]))]),
    os:putenv("HECATE_ARCHIVE_ROOT", Root),
    {ok, _} = seal_segments:start_link(),
    {ok, _} = report_gaps:start_link(),
    {ok, _} = collect_observations:start_link(),
    Root.

cleanup(Root) ->
    _ = [catch gen_server:stop(M)
         || M <- [collect_observations, report_gaps, seal_segments]],
    os:unsetenv("HECATE_ARCHIVE_ROOT"),
    _ = os:cmd("rm -rf " ++ Root),
    ok.

%% --- cases ---

%% CBOR is language-neutral, so atoms do not survive it: keys and atom values
%% come back as binaries. That is a property of the tape, not a defect — a record
%% written today must be readable in ten years by something that is not the BEAM —
%% and it is asserted here so a replay parser can rely on it.
lands(Root) ->
    Payload = <<"{\"results\":[]}">>,
    deliver(observation(?DATASET, ?EPOCH, 0, Payload)),
    [Rec] = records(Root, ?DATASET),
    ?assertEqual(<<"observation">>, maps:get(<<"type">>, Rec)),
    ?assertEqual(?SOURCE, maps:get(<<"source">>, Rec)),
    ?assertEqual(?DATASET, maps:get(<<"dataset">>, Rec)),
    ?assertEqual(?EPOCH, maps:get(<<"epoch">>, Rec)),
    ?assertEqual(0, maps:get(<<"seq">>, Rec)),
    ?assertEqual(1753303500204, maps:get(<<"response_at">>, Rec)),
    %% The one that matters: the upstream bytes came back byte for byte.
    ?assertEqual(Payload, maps:get(<<"payload">>, Rec)),
    ?assertEqual(binary:encode_hex(crypto:hash(sha256, Payload), lowercase),
                 maps:get(<<"payload_sha256">>, Rec)).

bad_hash(Root) ->
    Ds = <<"ods169">>,
    Fact = observation(Ds, ?EPOCH, 0, <<"honest">>),
    deliver(Fact#{payload => <<"tampered">>}),
    ?assertEqual([], records(Root, Ds)).

redelivery(Root) ->
    Ds = <<"ods002">>,
    Fact = observation(Ds, ?EPOCH, 0, <<"once">>),
    deliver(Fact),
    deliver(Fact),
    ?assertEqual(1, length(records(Root, Ds))),
    %% Same position in the stream, different bytes: a contradiction, and neither
    %% version is silently preferred.
    deliver(observation(Ds, ?EPOCH, 0, <<"different">>)),
    ?assertEqual(1, length(records(Root, Ds))).

gap(Root) ->
    Ds = <<"ods086">>,
    deliver(observation(Ds, ?EPOCH, 0, <<"a">>)),
    deliver(observation(Ds, ?EPOCH, 3, <<"b">>)),
    [Entry] = ledger(Root, Ds),
    ?assertEqual(<<"gap">>, maps:get(<<"kind">>, Entry)),
    ?assertEqual(2, maps:get(<<"missing">>, Entry)),
    ?assertEqual(0, maps:get(<<"after_seq">>, Entry)),
    ?assertEqual(3, maps:get(<<"before_seq">>, Entry)).

restart(Root) ->
    Ds = <<"ods087">>,
    deliver(observation(Ds, ?EPOCH, 7, <<"before">>)),
    %% Sequence restarts from zero under a NEW epoch. That is a restart, about
    %% which nothing can be claimed, not eight missing facts.
    deliver(observation(Ds, ?EPOCH + 5000, 0, <<"after">>)),
    [Entry] = ledger(Root, Ds),
    ?assertEqual(<<"epoch">>, maps:get(<<"kind">>, Entry)),
    ?assertEqual(?EPOCH, maps:get(<<"was_epoch">>, Entry)),
    ?assertEqual(2, length(records(Root, Ds))).

%% --- helpers ---

observation(Dataset, Epoch, Seq, Payload) ->
    #{type           => observation,
      schema_v       => 1,
      source         => ?SOURCE,
      dataset        => Dataset,
      epoch          => Epoch,
      seq            => Seq,
      endpoint       => <<"https://opendata.elia.be/">>,
      request_at     => 1753303499512,
      response_at    => 1753303500204,
      status         => 200,
      content_type   => <<"application/json">>,
      payload        => Payload,
      payload_sha256 => binary:encode_hex(crypto:hash(sha256, Payload), lowercase),
      sensor_ref     => <<"a1b2c3d">>,
      from           => <<"hecate-grid">>}.

%% Deliver, then serialise on both servers so the assertions see settled state.
deliver(Fact) ->
    collect_observations ! {macula_event, make_ref(), <<"archive/observations">>,
                            Fact, #{}},
    _ = catch gen_server:call(collect_observations, sync),
    _ = catch gen_server:call(report_gaps, sync),
    ok.

records(Root, Dataset) ->
    [unpack(B) || B <- frames(read_segments(Root, Dataset))].

read_segments(Root, Dataset) ->
    Paths = filelib:wildcard(filename:join([Root, binary_to_list(?SOURCE),
                                            binary_to_list(Dataset),
                                            "*", "*", "*", "*.cbor"])),
    << <<(element(2, {ok, _} = file:read_file(P)))/binary>> || P <- Paths >>.

frames(<<>>) ->
    [];
frames(<<Len:32/big, Rec:Len/binary, Rest/binary>>) ->
    [Rec | frames(Rest)].

unpack(Bin) ->
    {ok, Term} = macula_cbor_nif:unpack(Bin),
    Term.

ledger(Root, Dataset) ->
    Path = filename:join([Root, binary_to_list(?SOURCE), binary_to_list(Dataset),
                          "GAPS.jsonl"]),
    lines(file:read_file(Path)).

lines({ok, Bin}) ->
    [json:decode(L) || L <- binary:split(Bin, <<"\n">>, [global]), L =/= <<>>];
lines({error, enoent}) ->
    [].
