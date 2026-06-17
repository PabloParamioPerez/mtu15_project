# STATUS: ALIVE
# LAST-AUDIT: 2026-06-09
# FEEDS: thesis/presentations/.../slides.tex --- per-session DA-IDA wedge OLS
#        at HOURLY frequency, PLACEBO cutovers (shifted 1 year earlier).
#        Mirrors ols_wedge_per_session_hourly.R but cutover at 2024-03-19
#        (ID15 plb, 40d post) and 2024-10-01 (DA15 plb, 92d post).
#
# Standard spec: hourly + year-by-renewable
#   - ID15 plb headline: pool 2023+2024 (analogous to pooling 2024+2025 at headline).
#   - DA15 plb headline: per-year separate.
#
# NB: per-session ID15 placebo (cutover 2024-03-19) requires pre-window in
#     the 3-session regime, which only exists from 2024-06-14 -> impossible.
#     ID15 placebo only runs for wedge_agg.
#
# OUT: results/regressions/bid/mtu15_critical_flat/ols_wedge_per_session_hourly_placebo.csv

suppressPackageStartupMessages({ library(arrow); library(lmtest); library(sandwich) })
repo <- "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"

ps_panel <- read_parquet(file.path(repo, "data/derived/panels/bsts_per_session_hourly_panel.parquet"))
ps_panel$d <- as.Date(ps_panel$d); ps_panel <- ps_panel[order(ps_panel$d, ps_panel$hour), ]
ps_panel$wind_gwh  <- ps_panel$wind_mwh  / 1000
ps_panel$solar_gwh <- ps_panel$solar_mwh / 1000

agg_panel <- read_parquet(file.path(repo, "data/derived/panels/bsts_hourly_panel.parquet"))
agg_panel$d <- as.Date(agg_panel$d); agg_panel <- agg_panel[order(agg_panel$d, agg_panel$hour), ]
agg_panel$wind_gwh  <- agg_panel$wind_mwh  / 1000
agg_panel$solar_gwh <- agg_panel$solar_mwh / 1000
agg_panel$wedge_agg_h <- agg_panel$da_price_eur - agg_panel$ida_price_eur

run_ols <- function(panel, response, pre_start, post_start, post_end, tag) {
  pre_lo <- as.Date(pre_start); cutover <- as.Date(post_start); pe <- as.Date(post_end)
  sub <- panel[panel$d >= pre_lo & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, "wind_gwh", "solar_gwh", "gas_eur")]), ]
  sub$year   <- as.integer(format(sub$d, "%Y"))
  sub$month  <- factor(format(sub$d, "%m"))
  sub$dow    <- factor(format(sub$d, "%u"))
  sub$hour_f <- factor(sub$hour)
  sub$post   <- as.integer(sub$d >= cutover)
  sub$t      <- as.integer(sub$d - min(sub$d))
  sub$y2023  <- as.integer(sub$year == 2023)
  sub$y2024  <- as.integer(sub$year == 2024)
  sub$y2023p <- as.integer(sub$year >= 2023)

  fmt <- function(m, spec_name) {
    s <- coeftest(m, vcov=NeweyWest(m, lag=24*7, prewhite=FALSE))
    c1 <- s["post", c("Estimate", "Std. Error", "Pr(>|t|)")]
    cat(sprintf("  %-50s eff=%+7.2f  SE=%5.2f  p=%.3f\n", spec_name, c1[1], c1[2], c1[3]))
    data.frame(tag=tag, spec=spec_name, eff=c1[1], se=c1[2], p=c1[3], n=nobs(m),
               stringsAsFactors=FALSE)
  }

  rows <- list()
  cat(sprintf("\n=== %s hourly placebo (n=%d, n_post=%d) ===\n", tag, nrow(sub), sum(sub$post)))
  rows[[1]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       hour_f + month + dow, data=sub),
                    "OLS hourly --- base (hour + month + DOW)")
  rows[[2]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       wind_gwh:y2024 + solar_gwh:y2024 +
                       t + hour_f + month + dow, data=sub),
                    "OLS hourly --- + year-by-renew (per year)")
  rows[[3]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       wind_gwh:y2023p + solar_gwh:y2023p +
                       t + hour_f + month + dow, data=sub),
                    "OLS hourly --- + year-by-renew (2023+24 pooled)")
  do.call(rbind, rows)
}

specs <- list(
  # ID15 placebo: cutover 2024-03-19, 40d post -> 2024-04-27.
  # wedge_agg uses pre 2022-01-01 (aggregate IDA price exists throughout).
  # Per-session NOT possible (no 3-session regime pre).
  list(panel=agg_panel, tag="ID15 wedge_agg_h (plb)", resp="wedge_agg_h",
       pre="2022-01-01", post="2024-03-19", end="2024-04-27"),
  # DA15 placebo: cutover 2024-10-01, 92d post -> 2024-12-31.
  list(panel=agg_panel, tag="DA15 wedge_agg_h (plb)", resp="wedge_agg_h",
       pre="2022-01-01", post="2024-10-01", end="2024-12-31"),
  list(panel=ps_panel, tag="DA15 wedge_s1_h (plb)", resp="wedge_s1_h",
       pre="2024-06-14", post="2024-10-01", end="2024-12-31"),
  list(panel=ps_panel, tag="DA15 wedge_s2_h (plb)", resp="wedge_s2_h",
       pre="2024-06-14", post="2024-10-01", end="2024-12-31"),
  list(panel=ps_panel, tag="DA15 wedge_s3_h (plb)", resp="wedge_s3_h",
       pre="2024-06-14", post="2024-10-01", end="2024-12-31")
)

all_rows <- list()
for (s in specs) all_rows[[length(all_rows)+1]] <- run_ols(s$panel, s$resp, s$pre, s$post, s$end, s$tag)
out <- do.call(rbind, all_rows)
write.csv(out, file.path(repo, "results/regressions/bid/mtu15_critical_flat/ols_wedge_per_session_hourly_placebo.csv"),
          row.names=FALSE)
cat("\nWrote ols_wedge_per_session_hourly_placebo.csv\n")
