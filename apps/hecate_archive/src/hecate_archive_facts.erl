%%% @doc What the archive itself says out loud.
%%%
%%% The archive is a consumer of observations, but it is a PRODUCER of two facts
%%% about its own integrity:
%%%
%%%   archive_gap    — a discontinuity in a stream's sequence. Something was
%%%                    published and never arrived.
%%%   segment_sealed — a segment closed, with its checksum.
%%%
%%% Publishing the gap is the point. An archive that only records its holes to
%%% local disk is an archive whose holes nobody sees until someone goes looking,
%%% which is normally after a claim has already been built on the data. On the
%%% mesh, a gap is visible to the realm and to anyone watching, at the time it
%%% happens.
%%%
%%% Degrades silently while the mesh is dark: an unreachable mesh must never stop
%%% the tape from being written. The disk ledger is the record; the fact is the
%%% announcement.
-module(hecate_archive_facts).

-export([gaps_topic/0, seals_topic/0, gap/1, sealed/1]).

-define(REPORTER, <<"hecate-archive">>).

%% @doc Topic carrying discontinuities in a stream's sequence.
-spec gaps_topic() -> binary().
gaps_topic() -> <<"archive/gaps">>.

%% @doc Topic carrying segment seals (a segment closed, here is its checksum).
-spec seals_topic() -> binary().
seals_topic() -> <<"archive/seals">>.

%% @doc Announce a discontinuity: `Missing' facts between `after_seq' and
%% `before_seq' were published by the sensor and never reached the tape.
-spec gap(map()) -> ok.
gap(G) when is_map(G) ->
    publish(gaps_topic(),
            G#{type => archive_gap,
               from => ?REPORTER,
               at   => erlang:system_time(millisecond)}).

%% @doc Announce a sealed segment. The checksum is over the UNCOMPRESSED record
%% stream, so it stays meaningful if the compression ever changes.
-spec sealed(map()) -> ok.
sealed(S) when is_map(S) ->
    publish(seals_topic(),
            S#{type => segment_sealed,
               from => ?REPORTER,
               at   => erlang:system_time(millisecond)}).

%% --- Internal ---

%% The lookup itself is caught, not just its result. The mesh subsystem may be
%% absent entirely (not started yet, or torn down before us), and an announcement
%% that can crash its caller would take the TAPE down with it. Sealing a segment
%% must never depend on anything outside this machine.
publish(Topic, Fact) ->
    emit(catch {hecate_om:macula_client(), hecate_om_identity:realm()}, Topic, Fact).

emit({{ok, Pool}, {ok, Realm}}, Topic, Fact) ->
    catch macula:publish(Pool, Realm, Topic, Fact),
    ok;
emit(_DarkOrNoRealm, _Topic, _Fact) ->
    ok.
