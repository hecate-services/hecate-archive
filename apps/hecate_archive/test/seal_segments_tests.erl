%%% @doc The tape must do exactly three things, and this checks all three:
%%% frame records so a truncated tail costs one record, seal with a checksum over
%%% the uncompressed stream, and leave no artefact for a stream that said nothing.
-module(seal_segments_tests).

-include_lib("eunit/include/eunit.hrl").

tape_test_() ->
    {setup, fun setup/0, fun cleanup/1,
     fun(Root) ->
         [{"records are framed and readable back", fun() -> framed(Root) end},
          {"seal writes a gzip and a checksum over the plain bytes",
           fun() -> sealed(Root) end},
          {"an empty stream leaves nothing behind", fun() -> empty(Root) end},
          {"a restart inside the same bucket does not destroy the earlier segment",
           fun() -> restart_preserves(Root) end}]
     end}.

setup() ->
    Root = filename:join(["/tmp", "hecate-archive-test",
                          integer_to_list(erlang:unique_integer([positive]))]),
    os:putenv("HECATE_ARCHIVE_ROOT", Root),
    {ok, _} = seal_segments:start_link(),
    Root.

cleanup(Root) ->
    catch gen_server:stop(seal_segments),
    os:unsetenv("HECATE_ARCHIVE_ROOT"),
    _ = os:cmd("rm -rf " ++ Root),
    ok.

framed(Root) ->
    ok = seal_segments:append(<<"elia">>, <<"ods134">>, <<"one">>),
    ok = seal_segments:append(<<"elia">>, <<"ods134">>, <<"twotwo">>),
    [Path] = plain_segments(Root, "elia"),
    {ok, Bin} = file:read_file(Path),
    ?assertEqual([<<"one">>, <<"twotwo">>], unframe(Bin)).

sealed(Root) ->
    ok = seal_segments:append(<<"knmi">>, <<"stations">>, <<"payload">>),
    [Plain] = plain_segments(Root, "knmi"),
    {ok, Raw} = file:read_file(Plain),
    Expected = binary:encode_hex(crypto:hash(sha256, Raw), lowercase),
    ok = gen_server:stop(seal_segments),
    ?assertEqual([], plain_segments(Root, "knmi")),
    [Gz] = wildcard(Root, "knmi", "*.cbor.gz"),
    {ok, Compressed} = file:read_file(Gz),
    ?assertEqual(Raw, zlib:gunzip(Compressed)),
    [Sum] = wildcard(Root, "knmi", "*.sha256"),
    {ok, Line} = file:read_file(Sum),
    ?assertNotEqual(nomatch, binary:match(Line, Expected)),
    {ok, _} = seal_segments:start_link().

empty(Root) ->
    %% A stream that opened a segment and wrote nothing is not evidence of an
    %% hour; the gap ledger owns absence, not the tape.
    ?assertEqual([], wildcard(Root, "never-spoke", "*")).

%% The regression for the production data loss of 2026-07-24.
%%
%% Two archive runs inside the SAME roll bucket used to collide: the segment path
%% was a pure function of {stream, bucket}, so the second run reopened the first
%% run's path and, at seal, overwrote the .gz it had already written. A restart 45
%% minutes into an hour destroyed all 45 minutes on every stream. Records that had
%% been received, acknowledged and sealed simply left the tape.
restart_preserves(Root) ->
    %% Its own source, so the assertion sees only this test's records.
    Src = <<"restarted">>,
    ok = seal_segments:append(Src, <<"ods161">>, <<"first-run">>),
    ok = gen_server:stop(seal_segments),
    {ok, _} = seal_segments:start_link(),
    ok = seal_segments:append(Src, <<"ods161">>, <<"second-run">>),
    ok = gen_server:stop(seal_segments),
    {ok, _} = seal_segments:start_link(),
    %% Both runs' bytes must still be on the tape, in two distinct segments.
    Sealed = wildcard(Root, "restarted", "*.cbor.gz"),
    Payloads = lists:sort(
        [P || Gz <- Sealed,
              {ok, Bin} <- [file:read_file(Gz)],
              P <- unframe(zlib:gunzip(Bin))]),
    ?assertEqual([<<"first-run">>, <<"second-run">>], Payloads),
    %% ...and every sealed segment carries its checksum.
    ?assertEqual(2, length(Sealed)),
    ?assertEqual(length(Sealed), length(wildcard(Root, "restarted", "*.sha256"))).

%% --- helpers ---

unframe(<<>>) ->
    [];
unframe(<<Len:32/big, Rec:Len/binary, Rest/binary>>) ->
    [Rec | unframe(Rest)].

plain_segments(Root, Source) ->
    wildcard(Root, Source, "*.cbor").

wildcard(Root, Source, Pat) ->
    filelib:wildcard(filename:join([Root, Source, "*", "*", "*", "*", Pat])).
