# Archived one-shot resync scripts

These three bash scripts (`_resync_failed.sh`, `_resync_v2.sh`,
`_resync_v3.sh`) were used during the 2026-04-27 catch-up download
session to recover from rate-limit failures and to fetch the additional
ENTSO-E datasets needed for F14/F15/F16 + B6 audit. They are kept here
as a historical record of the actual download sequence.

**Do not run.** They have hardcoded month windows and call into
`_generic_sync.py` which has since been updated (added `--control-domain`
and `--area-domain` flags that v2/v3 don't use).

For a clean reproduction of the full ENTSO-E raw-data sweep, use
`scripts/pipelines/entsoe/_sync_all.py` instead. It is idempotent
(skips files already on disk), supports a `--only` flag to run a subset,
and uses the corrected parameter conventions.
