# hecate-archive

**The collector.** Sensors observe the world and publish what they saw; this keeps it,
append-only, forever.

Same cardinality as [hecate-warden](https://codeberg.org/hecate-services/hecate-warden)
to [hecate-sentinel](https://codeberg.org/hecate-services/hecate-sentinel): many
lightweight observers, one service that holds the record.

```
hecate-grid     (Elia, Elexon, Fingrid, ...)  ┐
hecate-weather  (Open-Meteo, DWD, ...)        ├── observation facts ──▶  hecate-archive
hecate-<next>                                 ┘                         (one service,
                                                                         many streams)
```

## Why it exists

Some data cannot be re-obtained. A grid operator revises published values after the
fact, a sensor network drops the hour a storm took its power out, an endpoint changes
shape between one quarter and the next. History is not a thing you can go and fetch
later; it is a thing you either wrote down at the time or lost.

So this writes it down at the time, and does two unfashionable things while doing it.

### It keeps the bytes, not the meaning

An observation carries the **verbatim upstream response**. The archive does not parse
it, scale it, or pick fields out of it. Parsing happens later, offline, against the
tape.

That is not laziness, it is the lesson the research programme this serves paid for
twice. An ingest that is wrong produces a record that is wrong **and internally
consistent**, and no amount of analysis on that record can find the error. Keep the
bytes, and a parser bug is a re-run. Keep only the parse, and it is a retraction.

It is also the cheaper option, which is the happy part: a sensor that does not parse is
lighter than one that does, so the correct choice and the light choice coincide.

### It publishes its own holes

Every observation carries a sequence number. The archive tracks the high-water mark per
stream and records every discontinuity, to `GAPS.jsonl` beside the data and as an
`archive_gap` fact on the mesh.

Without that, a dropped fact is indistinguishable from a quiet hour, and an archive that
hides its holes is worse than no archive, because it is trusted. A hole should be a
recorded event, not an absence nobody notices.

## What lands on disk

```
/bulk0/hecate-archive/<source>/<dataset>/<YYYY>/<MM>/<DD>/<source>-<YYYYMMDD>T<HHMM>.cbor.gz
                                                          <source>-<YYYYMMDD>T<HHMM>.sha256
                     <source>/<dataset>/GAPS.jsonl
```

- **Records are length-prefixed CBOR**, `<<Len:32/big, Cbor:Len/binary>>`, so a truncated
  tail from an unclean shutdown costs one record rather than a segment.
- **Segments roll hourly on ARRIVAL wall-clock**, never on a record's own timestamps. A
  late observation would otherwise target a sealed segment, and a segment that can be
  reopened is not sealed. Nothing is lost: every record carries its own `request_at` and
  `response_at` inside, so segment boundaries are storage, never a temporal claim.
- **Sealed means gzipped, checksummed, closed.** The SHA-256 is over the *uncompressed*
  record stream, computed as records land, so it stays meaningful if the compression
  ever changes and costs no re-read. The `.sha256` is in `sha256sum(1)` format: gunzip
  and `sha256sum -c` verifies the tape with no bespoke tooling.
- **A segment with no `.sha256` is unclean.** It is flagged, never repaired. A tape that
  quietly patches itself is a tape you cannot cite.
- **An empty hour leaves no artefact**, so the presence of a segment always means records
  arrived. Absence is the gap ledger's business.

## The observation fact

Sensors publish this on `archive/observations`:

```erlang
#{type           => observation,
  schema_v       => 1,
  source         => <<"elia">>,      %% stable id, never a URL
  dataset        => <<"ods161">>,    %% the source's own dataset id
  epoch          => 1753303400000,   %% the sensor RUN this seq belongs to
  seq            => 918273,          %% monotonic per {source, dataset} within a run
  endpoint       => <<"https://...">>,
  request_at     => 1753303499512,   %% when the sensor asked
  response_at    => 1753303500204,   %% when the answer landed
  status         => 200,
  content_type   => <<"application/json">>,
  payload        => <<"...">>,       %% VERBATIM
  payload_sha256 => <<"76e382...">>,
  sensor_ref     => <<"a1b2c3d">>,   %% git sha of the sensor build
  from           => <<"hecate-grid">>}
```

**On the tape, keys are binaries.** CBOR is language-neutral, so atoms do not survive it:
`type => observation` is stored as `<<"type">> => <<"observation">>`. That is a property
worth relying on rather than working around, because a record written today has to be
readable in ten years by something that is not the BEAM. A replay parser should expect
binary keys and binary enum values.

Four fields carry more weight than they look like they do.

**No event time.** When each row happened is inside the payload, and extracting it is
interpretation. The envelope carries only what the sensor knows first-hand.

**`request_at` and `response_at` both.** Upstream latency is free to record now and
impossible to reconstruct later. `response_at` is the observation time an experiment must
filter on, and it is what makes a lookahead-free feature vector possible at all.

**`epoch` scopes `seq` to one run of the sensor.** Sensors hold no store, so a counter
restarts when they do. Without the epoch, a restart is indistinguishable from mass data
loss; with it, the archive records "the sensor restarted" (about which it claims nothing)
separately from "facts went missing" (which it counts).

**`sensor_ref` is the build that produced the record.** The capture harness is a runner
and can be wrong. When it is, the damage must be bounded to a known interval instead of
smeared anonymously across the tape.

## What it checks before writing

1. The envelope keys a stream (`source`, `dataset`, `epoch`, `seq`), or it is dropped.
2. `payload_sha256` matches the payload actually received, or it is dropped. This is what
   makes the tape's contents attributable to the upstream response rather than to
   whatever survived the wire.
3. This exact `{source, dataset, epoch, seq}` is not already on the tape. Delivery is
   at-least-once and a reconnect re-delivers, so duplicates are expected and cheap.
4. A duplicate seq carrying a **different** payload hash is not a duplicate, it is a
   contradiction. Logged as an error, neither version silently preferred.

The gap ledger is told **after** the append succeeds, never before, so a write failure
leaves a hole the ledger will notice rather than one it has been told to expect.

## Configuration

| Variable | Default | What |
|---|---|---|
| `HECATE_ARCHIVE_ROOT` | `/bulk0/hecate-archive` | Where the tape lives. On a beam node this must be a `/bulk` mount: the eMMC root is for the OS, and an archive that fills it takes the node down. |
| `HECATE_ARCHIVE_TOPIC` | `archive/observations` | The topic sensors publish on. |
| `HECATE_ARCHIVE_ROLL_MS` | `3600000` | Segment roll period. |
| `HECATE_ARCHIVE_MAX_SEEN` | `20000` | Dedupe window size, in records. |
| `HECATE_REALM` | (required) | Must match the sensors' realm, or the archive records a perfect, empty, honest tape. |
| `HECATE_HEALTH_PORT` | `8471` | `/health`. |

Health is the **tape's** health and nothing else. A quiet sensor is not a failure here
(that is the ledger's job to record); a tape that cannot be written to is, because every
fact arriving during that window is lost and unrecoverable.

## Known limits in 0.1

Stated rather than discovered later:

- **The mesh is on the capture path.** A fact that never arrives is never archived.
  Sequence numbers make that loss *visible*, which is the difference that matters, but
  they do not prevent it. Before starting a capture anyone intends to build on, soak the
  path and confirm the gap ledger is clean.
- **No replay request path.** A sensor cannot be asked to re-send what was lost, so a
  gap stays a gap. Pull-based sensors work around this by asking for overlapping windows,
  which recovers a missed poll but not a lost publish.
- **Splits are not enforced.** `splits.json` (see `DESIGN_ARCHIVE_CONTRACT.md`) is a
  commitment made in git at capture time; nothing in this service checks that an
  experiment honours it.

## Build

```sh
rebar3 compile
rebar3 eunit
rebar3 lint
rebar3 as prod release
```

See [`DESIGN_ARCHIVE_CONTRACT.md`](DESIGN_ARCHIVE_CONTRACT.md) for the full contract and
the reasoning behind each choice.
