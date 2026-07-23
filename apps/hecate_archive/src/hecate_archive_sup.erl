%%% @doc Top supervisor for hecate_archive.
%%%
%%% Three children, started in dependency order, each owning one capability:
%%%
%%%   seal_segments        — the tape itself: open segment per stream, framed
%%%                          append, hourly roll, seal + checksum.
%%%   report_gaps          — the sequence ledger: what the tape is MISSING, on
%%%                          disk and on the mesh.
%%%   collect_observations — the mesh subscriber that feeds both.
%%%
%%% `rest_for_one': if the tape dies, the subscriber must go down with it rather
%%% than acknowledge facts it cannot persist. Losing a fact loudly is correct;
%%% losing it silently is the failure this whole service exists to prevent.
-module(hecate_archive_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => rest_for_one, intensity => 5, period => 10},
    Children = [
        worker(seal_segments),
        worker(report_gaps),
        worker(collect_observations)
    ],
    {ok, {SupFlags, Children}}.

worker(Module) ->
    #{id => Module,
      start => {Module, start_link, []},
      restart => permanent,
      %% Generous shutdown: seal_segments seals every open segment on the way
      %% out, and an unsealed segment is an unclean segment.
      shutdown => 30000,
      type => worker,
      modules => [Module]}.
