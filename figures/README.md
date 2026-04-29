# `figures/` — all figures

Single canonical location for figures. Subfolders are by *destination*, not by topic.

## Subfolders

- **`thesis/`** — final figures referenced by the thesis text or by a thesis-grade slide. Numbered (e.g. `fig01_S5_four_panel_concordance.pdf`) and committed to the repo so the thesis is reproducible from the repo alone.
- **`presentation/`** — figures used only in workshop / defense presentations that are not also in the thesis. Often more verbose annotations than the thesis versions.
- **`working/`** — work-in-progress figures from analysis. Anything still in flux. Move to `thesis/` or `presentation/` only when the underlying claim has settled.
- **`attic/`** — figures from retired claims. Keep for transparency.

## Conventions

- Each figure is committed in **both** `.pdf` and `.png` formats. PDF for thesis/presentation embedding; PNG for quick previews and notebook outputs.
- Scripts that produce figures should write to `PROJECT / "figures" / "<dest>" / "<name>.{pdf,png}"`. Do not write to `data/` or `results/`.
- When a figure is superseded, move the old version to `attic/` rather than overwriting in place — preserves provenance.
- Figure dimensions: keep below 2000 px on any side at 200 dpi (per `feedback_figure_dimensions.md` in `notebooks/memos/`). Common safe sizes: `figsize=(13.5, 5.5)` at default `dpi=140` ≈ 1890 × 770 px.
