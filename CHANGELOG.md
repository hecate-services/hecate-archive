# Changelog

All notable changes to `hecate-archive` are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-07-24

### Added
- First cut of the collector in the sensor family (many sensors, one archive),
  the same cardinality as warden to sentinel.
- `collect_observations`: mesh subscriber with envelope validation, payload hash
  verification, a bounded at-least-once dedupe window, and contradiction
  detection (same seq, different bytes).
- `seal_segments`: length-prefixed CBOR records, hourly roll on arrival
  wall-clock, gzip + SHA-256 at seal in `sha256sum(1)` format, empty segments
  dropped rather than sealed.
- `report_gaps`: per-stream high-water tracking, `GAPS.jsonl` beside the data,
  `archive_gap` facts on the mesh, and epoch-aware handling so a sensor restart
  is recorded as a restart rather than counted as data loss.
- `DESIGN_ARCHIVE_CONTRACT.md`: the capture contract, including why the archive
  keeps verbatim bytes rather than parsed values.
