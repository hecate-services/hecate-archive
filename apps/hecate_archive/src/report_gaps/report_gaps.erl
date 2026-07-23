%%% @doc The gap ledger: what the tape is MISSING, written down and said out loud.
%%%
%%% Every observation carries a `seq' that is monotonic per stream. This tracks
%%% the last one seen per stream and records every discontinuity, because without
%%% it a dropped fact is indistinguishable from a poll that never happened, and
%%% the tape lies by omission. That is the failure mode that matters: a silent
%%% hole looks exactly like a quiet hour, and an experiment run across it will
%%% never know.
%%%
%%% Two outputs, both required:
%%%
%%%   GAPS.jsonl   — append-only, beside the stream's segments, so the hole
%%%                  travels with the data it is a hole in.
%%%   archive_gap  — a fact on the mesh, so the hole is visible when it happens
%%%                  rather than when someone goes looking.
%%%
%%% A LATE record (a seq at or below the high-water mark, arriving after the
%%% dedupe window has forgotten it) is logged too. It is not a gap, but it is
%%% evidence the transport reordered, and that is worth knowing before a claim
%%% rests on ordering.
-module(report_gaps).
-behaviour(gen_server).

-export([start_link/0, observe/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(st, {root :: string(),
             high = #{} :: #{{binary(), binary()} => {integer(), integer()}}}).

%% @doc Note that `Seq' of run `Epoch' arrived for a stream. Asynchronous: the
%% ledger must never be able to slow the tape down.
-spec observe(binary(), binary(), integer(), integer()) -> ok.
observe(Source, Dataset, Epoch, Seq) ->
    gen_server:cast(?MODULE, {observe, Source, Dataset, Epoch, Seq}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, #st{root = seal_segments:root()}}.

handle_call(_Req, _From, St) -> {reply, {error, unknown_call}, St}.

handle_cast({observe, Source, Dataset, Epoch, Seq}, St) ->
    {noreply, note({Source, Dataset}, Epoch, Seq, St)};
handle_cast(_Msg, St) ->
    {noreply, St}.

handle_info(_Info, St) -> {noreply, St}.
terminate(_Reason, _St) -> ok.

%% --- Internal ---

note(Stream, Epoch, Seq, St) ->
    advance(maps:get(Stream, St#st.high, undefined), Stream, Epoch, Seq, St).

%% First sight of a stream. NOT a gap: we cannot know what happened before we
%% were listening, and inventing a gap back to zero would be a false claim about
%% the sensor rather than a true one about ourselves.
advance(undefined, {Source, Dataset} = Stream, Epoch, Seq, St) ->
    logger:info("[archive] stream ~ts/~ts opens at epoch ~b seq ~b",
                [Source, Dataset, Seq, Epoch]),
    high(Stream, Epoch, Seq, St);
advance({Epoch, Last}, Stream, Epoch, Seq, St) when Seq =:= Last + 1 ->
    high(Stream, Epoch, Seq, St);
advance({Epoch, Last}, Stream, Epoch, Seq, St) when Seq > Last + 1 ->
    record_gap(Stream, Last, Seq, St),
    high(Stream, Epoch, Seq, St);
%% A NEW epoch is the sensor restarting, not data going missing. Recorded,
%% because a restart is a discontinuity an experiment must know about, but never
%% counted as a gap: nothing can be said about what the sensor would have
%% published while it was not running.
advance({Was, _Last}, Stream, Epoch, Seq, St) when Epoch =/= Was ->
    record_epoch(Stream, Was, Epoch, Seq, St),
    high(Stream, Epoch, Seq, St);
advance({_Epoch, Last}, Stream, _E, Seq, St) ->
    record_late(Stream, Last, Seq, St),
    St.

high(Stream, Epoch, Seq, St) ->
    St#st{high = maps:put(Stream, {Epoch, Seq}, St#st.high)}.

record_gap({Source, Dataset} = Stream, Last, Seq, St) ->
    Missing = Seq - Last - 1,
    logger:warning("[archive] GAP ~ts/~ts: ~b missing between seq ~b and ~b",
                   [Source, Dataset, Missing, Last, Seq]),
    Entry = #{<<"kind">>       => <<"gap">>,
              <<"source">>     => Source,
              <<"dataset">>    => Dataset,
              <<"after_seq">>  => Last,
              <<"before_seq">> => Seq,
              <<"missing">>    => Missing,
              <<"at">>         => erlang:system_time(millisecond)},
    append_line(Stream, Entry, St),
    hecate_archive_facts:gap(#{source     => Source,
                               dataset    => Dataset,
                               after_seq  => Last,
                               before_seq => Seq,
                               missing    => Missing}).

record_epoch({Source, Dataset} = Stream, Was, Epoch, Seq, St) ->
    logger:notice("[archive] ~ts/~ts sensor restarted: epoch ~b -> ~b at seq ~b",
                  [Source, Dataset, Was, Epoch, Seq]),
    Entry = #{<<"kind">>       => <<"epoch">>,
              <<"source">>     => Source,
              <<"dataset">>    => Dataset,
              <<"was_epoch">>  => Was,
              <<"epoch">>      => Epoch,
              <<"seq">>        => Seq,
              <<"at">>         => erlang:system_time(millisecond)},
    append_line(Stream, Entry, St).

record_late({Source, Dataset} = Stream, Last, Seq, St) ->
    logger:notice("[archive] late/reordered ~ts/~ts: seq ~b behind high-water ~b",
                  [Source, Dataset, Seq, Last]),
    Entry = #{<<"kind">>       => <<"late">>,
              <<"source">>     => Source,
              <<"dataset">>    => Dataset,
              <<"seq">>        => Seq,
              <<"high_water">> => Last,
              <<"at">>         => erlang:system_time(millisecond)},
    append_line(Stream, Entry, St).

%% JSONL beside the segments: one line per entry, greppable in ten years by
%% something that is not the BEAM, and appended with no read-modify-write so a
%% crash mid-line costs one line.
append_line(Stream, Entry, St) ->
    Path = ledger_path(St#st.root, Stream),
    ok = filelib:ensure_dir(Path),
    Line = [json:encode(Entry), "\n"],
    wrote(file:write_file(Path, Line, [append]), Path).

wrote(ok, _Path) ->
    ok;
wrote({error, Reason}, Path) ->
    logger:error("[archive] cannot append gap ledger ~ts: ~p", [Path, Reason]),
    ok.

ledger_path(Root, {Source, Dataset}) ->
    filename:join([Root, safe(Source), safe(Dataset), "GAPS.jsonl"]).

safe(Bin) ->
    binary_to_list(binary:replace(Bin, [<<"/">>, <<"..">>, <<0>>], <<"_">>, [global])).
