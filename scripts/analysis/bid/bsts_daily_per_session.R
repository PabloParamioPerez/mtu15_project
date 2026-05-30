# STATUS: ALIVE
# LAST-AUDIT: 2026-05-29
# FEEDS: advisor_memo.tex sec 3.A / 4.A -- per-IDA-session BSTS on daily
#        clearing prices. Each of the three intraday auction sessions
#        clears at a substantively different price distribution; this
#        script runs BSTS independently on each session.
#
#   ID15 real    -- 3 sessions (post-IDA-reform architecture)
#   ID15 placebo 2026  -- 3 sessions (same calendar one year later)
#     (ID15 placebo 2024 is pre-IDA-reform and had 6 sessions; not
#      session-comparable, so omitted.)
#   DA15 real IDA-side    -- 3 sessions
#   DA15 placebo 2024 IDA-side -- 3 sessions
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_daily_per_session.csv

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

args <- commandArgs(trailingOnly = FALSE)
sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else
  "scripts/analysis/bid/bsts_daily_per_session.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))
out_dir <- file.path(repo, "results/regressions/bid/mtu15_critical_flat")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

panel <- read_parquet(file.path(repo,
  "data/derived/panels/bsts_per_session_panel.parquet"))
panel$d <- as.Date(panel$d)
panel <- panel[order(panel$d), ]
cat("Per-session daily panel:", nrow(panel), "days, range",
    as.character(min(panel$d)), "to", as.character(max(panel$d)), "\n\n")

COVARS <- c("wind_gwh", "solar_gwh", "gas_eur")


run_one <- function(response, pre_start, pre_end, post_start, post_end) {
  ps <- as.Date(pre_start); pe <- as.Date(post_end)
  cutover <- as.Date(post_start)
  sub <- panel[panel$d >= ps & panel$d <= pe, ]
  needed <- c(response, COVARS)
  sub <- sub[complete.cases(sub[, needed]), ]
  if (nrow(sub) < 30 || max(sub$d) < cutover) return(NULL)
  data_mat <- as.matrix(sub[, c(response, COVARS)])
  data_ts  <- zoo(data_mat, order.by = sub$d)
  set.seed(42)
  imp <- tryCatch(
    CausalImpact(data_ts, c(ps, cutover - 1), c(cutover, pe),
                 model.args = list(niter = 2000, nseasons = 7,
                                    season.duration = 1)),
    error = function(e) NULL)
  if (is.null(imp)) return(NULL)
  s <- imp$summary
  list(eff = s["Average", "AbsEffect"],
       lo  = s["Average", "AbsEffect.lower"],
       hi  = s["Average", "AbsEffect.upper"],
       rel = s["Average", "RelEffect"],
       p   = s$p[1],
       n_pre  = sum(sub$d < cutover),
       n_post = sum(sub$d >= cutover))
}


CFGS <- list(
  list("ID15", "real",    "2024-12-29", NA, "2025-03-19", "2025-04-27"),
  list("ID15", "placebo2026", "2025-12-29", NA, "2026-03-19", "2026-04-27"),
  list("DA15", "real",    "2025-07-13", NA, "2025-10-01", "2025-11-09"),
  list("DA15", "placebo", "2024-07-13", NA, "2024-10-01", "2024-11-09")
)


rows <- list()
for (cfg in CFGS) {
  reform <- cfg[[1]]; side <- cfg[[2]]
  pre_lo <- cfg[[3]]; post_lo <- cfg[[5]]; post_hi <- cfg[[6]]
  cat(sprintf("\n=== %s %s ===\n", reform, side))
  for (sess in 1:3) {
    response <- sprintf("ida_price_eur_s%d", sess)
    r <- run_one(response, pre_lo, NA, post_lo, post_hi)
    if (is.null(r)) {
      cat(sprintf("  session %d: insufficient data\n", sess)); next
    }
    cat(sprintf("  S%d  eff=%+7.2f  CI=[%+7.2f,%+7.2f]  p=%.3f  n=%d/%d\n",
                sess, r$eff, r$lo, r$hi, r$p, r$n_pre, r$n_post))
    rows[[length(rows) + 1]] <- data.frame(
      reform=reform, side=side, session=sess,
      eff=r$eff, lo=r$lo, hi=r$hi, rel=r$rel, p=r$p,
      n_pre=r$n_pre, n_post=r$n_post, stringsAsFactors=FALSE)
  }
}

out_df <- do.call(rbind, rows)
write.csv(out_df, file.path(out_dir, "bsts_daily_per_session.csv"),
          row.names = FALSE)
cat(sprintf("\nWrote %d rows to bsts_daily_per_session.csv\n", nrow(out_df)))
