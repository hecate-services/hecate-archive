%%% @doc hecate_archive OTP application entry.
%%%
%%% Collector-only: hecate_om:boot/1 connects the mesh and registers the
%%% service's capabilities + /health, then calls start/1. No store_id/0 or
%%% data_dir/0, so NO reckon-db is started. The archive's durability is a tape of
%%% sealed segment files, not an event store.
-module(hecate_archive_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    hecate_om:boot(hecate_archive_service).

stop(_State) ->
    ok.
