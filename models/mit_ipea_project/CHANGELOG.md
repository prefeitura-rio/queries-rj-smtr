# Changelog - MIT IPEA Project

## [1.0.0] - 2024-06-21

### Added
- Create models:
  - `vw_ticketing.sql` ([#333](https://github.com/prefeitura-rio/queries-rj-smtr/pull/333))
    -  View containing ticketing data cleaned for:
        - Card types that do not unique identify users (these include cash transactions)
        - Pre-identified card ids with extreme values
  - `h3_gps.sql` (`incremental`) ([#307](https://github.com/prefeitura-rio/queries-rj-smtr/pull/307))
    - Assigns each GPS ping to a H3 tile using vw_h3. This consolidates multiple GPS pings for a given vehicle into a single h3 tile `tile_entry_time` and `tile_exit_time`.
  - `vw_h3.sql` ([#307](https://github.com/prefeitura-rio/queries-rj-smtr/pull/307))
    - Utility view which converts geometry column from STRING to GEOGRAPHY type and adds `centriod` column.
  - `vw_vehicle_details.sql` ([#307](https://github.com/prefeitura-rio/queries-rj-smtr/pull/307))
    - Utility view. Takes `rj-smtr.veiculo.sppo_licenciamento` and aliases column names to english.
