# STATUS: ALIVE
# LAST-AUDIT: 2026-05-29
# FEEDS: thesis/provisional/advisor_memo.tex sec 4.A -- Spec A BSTS on
#        per-(tech, market, hour-class) cleared MWh. Extends the daily
#        per-tech Spec A to within-day resolution.
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_hour_class_cleared.csv

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

args <- commandArgs(trailingOnly = FALSE)
sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else
  "scripts/analysis/bid/bsts_hour_class_cleared.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))

panel <- read_parquet(file.path(repo,
  "data/derived/panels/bsts_hour_class_q_panel.parquet"))
panel$d <- as.Date(panel$d); panel <- panel[order(panel$d), ]

COVARS <- c("wind_gwh", "solar_gwh", "gas_eur")
TECHS <- c("ccgt", "hydro", "hydro_pump", "wind", "solar")
MARKETS <- c("da", "ida")
HCLASSES <- c("critical", "midday", "flat")

run_one <- function(response, pre_start, post_start, post_end) {
  ps <- as.Date(pre_start); pe <- as.Date(post_end)
  cutover <- as.Date(post_start)
  sub <- panel[panel$d >= ps & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, COVARS)]), ]
  if (nrow(sub) < 30 || max(sub$d) < cutover) return(NULL)
  data_mat <- as.matrix(sub[, c(response, COVARS)])
  data_ts  <- zoo(data_mat, order.by = sub$d)
  set.seed(42)
  imp <- tryCatch(CausalImpact(data_ts, c(ps, cutover - 1), c(cutover, pe),
                                model.args = list(niter = 2000, nseasons = 7,
                                                   season.duration = 1)),
                  error = function(e) NULL)
  if (is.null(imp)) return(NULL)
  s <- imp$summary
  list(eff = s["Average", "AbsEffect"],
       lo  = s["Average", "AbsEffect.lower"],
       hi  = s["Average", "AbsEffect.upper"],
       p   = s$p[1])
}

CFGS <- list(
  list("ID15", "real",    "2024-06-14", "2025-03-19", "2025-04-27"),
  list("ID15", "placebo", "2023-06-14", "2024-03-19", "2024-04-27"),
  list("DA15", "real",    "2025-04-28", "2025-10-01", "2025-11-09"),
  list("DA15", "placebo", "2024-04-28", "2024-10-01", "2024-11-09")
)

rows <- list()
for (cfg in CFGS) {
  reform <- cfg[[1]]; side <- cfg[[2]]
  pre_lo <- cfg[[3]]; post_lo <- cfg[[4]]; post_hi <- cfg[[5]]
  cat(sprintf("\n##### %s %s #####\n", reform, side))
  for (tech in TECHS) for (mkt in MARKETS) for (hc in HCLASSES) {
    resp <- sprintf("q_%s_mwh_%s_%s", tech, mkt, hc)
    if (!(resp %in% names(panel))) next
    r <- run_one(resp, pre_lo, post_lo, post_hi)
    if (is.null(r)) next
    cat(sprintf("  %-30s eff=%+9.2f  [%+9.2f,%+9.2f]  p=%.3f\n",
                resp, r$eff, r$lo, r$hi, r$p))
    rows[[length(rows)+1]] <- data.frame(
      reform=reform, side=side, tech=tech, market=mkt, hour_class=hc,
      eff=r$eff, lo=r$lo, hi=r$hi, p=r$p,
      stringsAsFactors=FALSE)
  }
}

out_df <- do.call(rbind, rows)
write.csv(out_df,
  file.path(repo, "results/regressions/bid/mtu15_critical_flat/bsts_hour_class_cleared.csv"),
  row.names = FALSE)
cat(sprintf("\nWrote %d rows\n", nrow(out_df)))
