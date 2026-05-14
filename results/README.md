# `results/` — analytical outputs

Code-dependent products of analysis scripts. **NOT data.** The contents of `results/` are regenerated when scripts change — do not treat them as canonical, do not edit them by hand, and do not put raw or processed datasets here. Panel parquet files belong in `data/derived/panels/` (see `data/derived/README.md`).

## Layout

```
results/
├── regressions/                # CSV outputs of regression scripts, subdirectorised by topic
│   ├── system/                 # system-level friction (B6/B7, S5–S8)
│   ├── firm/                   # firm-level strategic conduct (B1, F-series; further split into firm/{critical_hours_thesis,pdbf,b9,other}/)
│   ├── bid/                    # bid-shape and granularity tests
│   ├── balancing/              # aFRR / mFRR / nuclear-availability
│   ├── regulatory/             # RT2 + CNMC enforcement
│   └── descriptive/            # descriptive cross-checks (calibration, mass placement, market share)
├── attic/                      # retired analytical outputs from pre-pivot framings
│                               # (Lerner work, dead claims, old workshop tables)
└── README.md                   # this file
```

Topic subdirectories of `regressions/` mirror `scripts/analysis/` topics. New analyses should write to the matching topic subfolder.

## Conventions

- Each regression script writes its output to `PROJECT / "results" / "regressions" / "<topic>" / "<name>.csv"`. Do not bypass this convention.
- If a script produces multiple related CSVs (point estimates + per-firm matrix + monthly aggregate), put them in a sub-directory under the topic (e.g. `regressions/firm/critical_hours_thesis/`).
- Most CSVs are gitignored — `results/regressions/` is large and changes frequently. Tables that the thesis cites are typically rendered to LaTeX under `thesis/paper/tables/` (force-added) for reproducibility.

## `attic/` — retired outputs

Pre-pivot framings (Lerner work, dead claims). Kept for transparency. **Do not cite as live evidence.** Corresponding retired panels live in `data/derived/attic/`.

## What's NOT here anymore

Earlier project versions used `results/{robustness, summaries, tables}/` subfolders. Restructured in May 2026:

- Robustness parquets → `data/derived/attic/` (they're derived data, not analytical outputs)
- Run-summary markdown reports → `notebooks/memos/_audits.md` and `notebooks/memos/RESEARCH_DIARY.md`
- Thesis-grade tables → `thesis/paper/tables/` (force-added under the paper)
