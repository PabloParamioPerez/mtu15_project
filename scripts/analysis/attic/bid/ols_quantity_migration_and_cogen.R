# STATUS: ALIVE
# LAST-AUDIT: 2026-06-09
# FEEDS: appendix slide "Cleared quantities" of the June 2026 deck.
#
# Two complementary outcomes:
#   (a) MIGRATION: Q_granular - Q_other per (tech, reform).
#         ID15 -> mig = Q_ida - Q_da    (positive = migration toward IDA)
#         DA15 -> mig = Q_da  - Q_ida   (positive = migration toward DA)
#       Coefficient is in MWh/h (hourly) -> converted to GWh/day-equivalent.
#   (b) Cogen LEVELS (DA, IDA cross), to fill the new Cogen row that was
#       missing from the original quantity panel.
#
# Standard headline spec: hourly OLS + year-by-renewable
#   ID15 -> 2024+25 pooled
#   DA15 -> per-year separate
# Newey-West HAC lag = 24*7.
#
# OUT: results/regressions/bid/mtu15_critical_flat/ols_quantity_migration_and_cogen.csv

suppressPackageStartupMessages({ library(arrow); library(lmtest); library(sandwich) })
repo <- "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
panel <- read_parquet(file.path(repo, "data/derived/panels/bsts_hourly_panel_w_cogen_mig.parquet"))
panel$d <- as.Date(panel$d); panel <- panel[order(panel$d, panel$hour), ]
panel$wind_gwh  <- panel$wind_mwh  / 1000
panel$solar_gwh <- panel$solar_mwh / 1000

# Mirror Q_IDA - Q_DA = mig (ID15 sign). For DA15 we'll later flip.
TECHS <- c("ccgt", "hydro", "hydro_pump", "nuclear", "solar", "wind", "cogen")
# Make sure DA15-sign columns exist:
for (t in TECHS) {
  panel[[sprintf("q_%s_mig_da15_mwh", t)]] <-
    panel[[sprintf("q_%s_mwh_da", t)]] - panel[[sprintf("q_%s_mwh_ida", t)]]
  # ID15 sign already encoded by build_migration_panel.py as q_<t>_mig_mwh
  panel[[sprintf("q_%s_mig_id15_mwh", t)]] <-
    panel[[sprintf("q_%s_mig_mwh", t)]]
}

run_ols <- function(response, pre_start, post_start, post_end, tag, spec_label) {
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
  sub$y2025  <- as.integer(sub$year == 2025)
  sub$y2024p <- as.integer(sub$year >= 2024)

  if (spec_label == "id15_pooled") {
    f <- as.formula(paste(response,
      "~ post + wind_gwh + solar_gwh + gas_eur +",
      "  wind_gwh:y2024p + solar_gwh:y2024p +",
      "  t + hour_f + month + dow"))
  } else if (spec_label == "da15_peryear") {
    f <- as.formula(paste(response,
      "~ post + wind_gwh + solar_gwh + gas_eur +",
      "  wind_gwh:y2023 + solar_gwh:y2023 +",
      "  wind_gwh:y2024 + solar_gwh:y2024 +",
      "  wind_gwh:y2025 + solar_gwh:y2025 +",
      "  t + hour_f + month + dow"))
  } else stop("unknown spec_label")

  m <- lm(f, data = sub)
  s <- coeftest(m, vcov = NeweyWest(m, lag = 24*7, prewhite = FALSE))
  c1 <- s["post", c("Estimate", "Std. Error", "Pr(>|t|)")]
  eff_gwh_d <- c1[1] * 24 / 1000
  cat(sprintf("  %-40s %-20s eff=%+8.1f MWh/h (=%+6.2f GWh/d)  SE=%5.1f  p=%.3f\n",
              tag, spec_label, c1[1], eff_gwh_d, c1[2], c1[3]))
  data.frame(tag=tag, spec=spec_label,
             eff_mwh_h=c1[1], eff_gwh_d_eq=eff_gwh_d,
             se=c1[2], p=c1[3], n=nobs(m), stringsAsFactors=FALSE)
}

specs <- list()

# LEVELS for techs that were missing or have been recomputed (cogen, hydro_pump, hydro).
for (t in c("cogen", "hydro_pump", "hydro")) {
  for (mkt in c("da", "ida")) {
    specs[[length(specs)+1]] <- list(tag=sprintf("ID15 %s (%s)", toupper(mkt), t),
                                      resp=sprintf("q_%s_mwh_%s", t, mkt),
                                      pre="2022-01-01", post="2025-03-19", end="2025-04-27",
                                      spec="id15_pooled")
    specs[[length(specs)+1]] <- list(tag=sprintf("DA15 %s (%s)", toupper(mkt), t),
                                      resp=sprintf("q_%s_mwh_%s", t, mkt),
                                      pre="2022-01-01", post="2025-10-01", end="2025-12-31",
                                      spec="da15_peryear")
  }
}

# MIGRATION (granular - other) for all 6 techs
for (tech in TECHS) {
  specs[[length(specs)+1]] <- list(tag=sprintf("ID15 MIG (%s)", tech),
                                    resp=sprintf("q_%s_mig_id15_mwh", tech),
                                    pre="2022-01-01", post="2025-03-19", end="2025-04-27",
                                    spec="id15_pooled")
  specs[[length(specs)+1]] <- list(tag=sprintf("DA15 MIG (%s)", tech),
                                    resp=sprintf("q_%s_mig_da15_mwh", tech),
                                    pre="2022-01-01", post="2025-10-01", end="2025-12-31",
                                    spec="da15_peryear")
}

all_rows <- list()
for (s in specs) all_rows[[length(all_rows)+1]] <-
  run_ols(s$resp, s$pre, s$post, s$end, s$tag, s$spec)
out <- do.call(rbind, all_rows)
write.csv(out, file.path(repo, "results/regressions/bid/mtu15_critical_flat/ols_quantity_migration_and_cogen.csv"),
          row.names=FALSE)
cat("\nWrote ols_quantity_migration_and_cogen.csv\n")
