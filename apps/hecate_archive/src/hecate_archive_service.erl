%%% @doc Hecate Archive — implements the hecate_om_service behaviour.
%%%
%%% The COLLECTOR in the sensor family. Where a warden or a news sensor observes
%%% the world and publishes facts, this one subscribes and keeps them: many
%%% sensors, one archive. It is the sentinel's role, for a different kind of
%%% signal.
%%%
%%% What it keeps is the VERBATIM upstream payload a sensor saw, not a parsed
%%% interpretation of it. Parsing happens later, offline, against the tape. The
%%% reason is the programme's own hard lesson: an ingest that is wrong produces a
%%% record that is wrong AND internally consistent, and no analysis of that
%%% record can find the error. Keep the bytes, and a parser bug is a re-run
%%% instead of a retraction.
%%%
%%% NO STORE in the reckon sense: no store_id/0 + data_dir/0, so hecate_om:boot/1
%%% wires the mesh and starts no event store. Durability here is an append-only
%%% tape of sealed segment files, and nothing in the running system ever reads a
%%% sealed segment back. Reading is an offline activity.
-module(hecate_archive_service).
-behaviour(hecate_om_service).

-export([info/0, start/1, stop/1, health/0, capabilities/0, identity_spec/0]).

info() ->
    #{name        => <<"hecate-archive">>,
      version     => <<"0.1.0">>,
      description => <<"The collector: verbatim sensor observations, append-only, forever">>}.

start(_Opts) ->
    hecate_archive_sup:start_link().

stop(_State) ->
    ok.

%% Health is the tape's health, and nothing else. A sensor being quiet is not a
%% failure here (that is the gap ledger's job to record); a tape that cannot be
%% written to is, because every fact arriving during that window is lost and
%% unrecoverable.
health() ->
    seal_segments:health().

%% What the archive announces: it collects observations, and it reports the gaps
%% in what it collected. The second is as important as the first — an archive
%% that hides its holes is worse than no archive, because it is trusted.
capabilities() ->
    [#{name => <<"archive.collect_observations">>, version => 1},
     #{name => <<"archive.report_gaps">>, version => 1}].

%% The UCAN the archive asks the realm to mint: authority to publish its own gap
%% and seal facts, and nothing else. It SUBSCRIBES to observations; it never
%% publishes one, so it can never forge an observation it did not receive.
identity_spec() ->
    #{scope     => <<"archive">>,
      actions   => [<<"report">>],
      resources => [hecate_archive_facts:gaps_topic(),
                    hecate_archive_facts:seals_topic()],
      ttl_days  => 30}.
