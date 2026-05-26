# STATUS: ALIVE
# LAST-AUDIT: 2026-05-26
# FEEDS: advisor_memo.tex sec 5 robustness -- window-length sensitivity of
#        the ID15 IDA price BSTS effect. Markle-Huss used 80-day pre, which
#        peer review flagged as possibly too short.
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_id15_window_sensitivity.csv

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

args <- commandArgs(trailingOnly = FALSE)
sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else
  "scripts/analysis/bid/bsts_id15_window_sensitivity.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))
out_dir <- file.path(repo, "results/regressions/bid/mtu15_critical_flat")

panel <- read_parquet(file.path(repo,
  "data/derived/panels/bsts_quantities_panel.parquet"))
panel$d <- as.Date(panel$d)
panel <- panel[order(panel$d), ]

COVARS <- c("wind_gwh", "solar_gwh", "gas_eur")
REFORM <- as.Date("2025-03-19")
POST_END <- as.Date("2025-04-27")


run_one <- function(pre_days, tag) {
  ps <- REFORM - pre_days
  pe <- POST_END
  sub <- panel[panel$d >= ps & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c("ida_price_eur", COVARS)]), ]
  data_mat <- as.matrix(sub[, c("ida_price_eur", COVARS)])
  data_ts <- zoo(data_mat, order.by = sub$d)
  pre_period <- c(ps, REFORM - 1)
  post_period <- c(REFORM, pe)
  set.seed(42)
  imp <- CausalImpact(data_ts, pre_period, post_period,
                       model.args = list(niter = 2000, nseasons = 7,
                                          season.duration = 1))
  s <- imp$summary
  list(tag = tag, pre_days = pre_days,
       eff = s["Average","AbsEffect"], lo = s["Average","AbsEffect.lower"],
       hi = s["Average","AbsEffect.upper"], rel = s["Average","RelEffect"],
       p = s$p[1], n_pre = sum(sub$d < REFORM), n_post = sum(sub$d >= REFORM))
}


cases <- list(
  c(60, "60d (shorter than M-H)"),
  c(80, "80d (M-H baseline)"),
  c(120, "120d"),
  c(180, "180d"),
  c(365, "365d (1 year)"),
  c(730, "730d (2 years)")
)

cat(sprintf("%-30s  %5s  %8s  %8s  %8s  %5s  %8s\n",
            "Pre window", "n_pre", "Eff", "CI lo", "CI hi", "p", "n_post"))
cat(strrep("-", 80), "\n")

rows <- list()
for (case in cases) {
  pd <- as.numeric(case[1])
  tag <- case[2]
  r <- run_one(pd, tag)
  rows[[length(rows)+1]] <- data.frame(
    tag = tag, pre_days = pd, eff = r$eff, lo = r$lo, hi = r$hi,
    rel = r$rel, p = r$p, n_pre = r$n_pre, n_post = r$n_post,
    stringsAsFactors = FALSE)
  cat(sprintf("%-30s  %5d  %+8.3f  %+8.3f  %+8.3f  %.3f  %8d\n",
              tag, r$n_pre, r$eff, r$lo, r$hi, r$p, r$n_post))
}

out_df <- do.call(rbind, rows)
write.csv(out_df, file.path(out_dir, "bsts_id15_window_sensitivity.csv"),
           row.names = FALSE)
cat("\nWrote bsts_id15_window_sensitivity.csv\n")
