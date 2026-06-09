# STATUS: ALIVE
# LAST-AUDIT: 2026-06-09
# FEEDS: appendix slide "Cleared quantities" of the June 2026 deck.
#
# MINIMAL re-run: only what's missing or changed after splitting Hydro_pump
# out of the hourly panel's Hydro column.
#   - Hydro_pump (new tech in hourly panel): 4 level cells + 2 migration cells
#   - Hydro (corrected to exclude pump in the hourly panel): 4 + 2
# 12 hourly regressions, standard headline spec, Newey-West HAC lag = 24*7.
#
# OUT: results/regressions/bid/mtu15_critical_flat/ols_quantity_hydropump_only.csv

suppressPackageStartupMessages({ library(arrow); library(lmtest); library(sandwich) })
repo <- "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
panel <- read_parquet(file.path(repo, "data/derived/panels/bsts_hourly_panel_w_cogen_mig.parquet"))
panel$d <- as.Date(panel$d); panel <- panel[order(panel$d, panel$hour), ]
panel$wind_gwh  <- panel$wind_mwh  / 1000
panel$solar_gwh <- panel$solar_mwh / 1000
# DA15-signed mig (positive = toward DA)
for (t in c("hydro", "hydro_pump")) {
  panel[[sprintf("q_%s_mig_id15_mwh", t)]] <-  panel[[sprintf("q_%s_mig_mwh", t)]]
  panel[[sprintf("q_%s_mig_da15_mwh", t)]] <- -panel[[sprintf("q_%s_mig_mwh", t)]]
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
  } else {
    f <- as.formula(paste(response,
      "~ post + wind_gwh + solar_gwh + gas_eur +",
      "  wind_gwh:y2023 + solar_gwh:y2023 +",
      "  wind_gwh:y2024 + solar_gwh:y2024 +",
      "  wind_gwh:y2025 + solar_gwh:y2025 +",
      "  t + hour_f + month + dow"))
  }
  m <- lm(f, data = sub)
  s <- coeftest(m, vcov = NeweyWest(m, lag = 24*7, prewhite = FALSE))
  c1 <- s["post", c("Estimate", "Std. Error", "Pr(>|t|)")]
  eff_gwh_d <- c1[1] * 24 / 1000
  cat(sprintf("  %-32s %-13s eff=%+8.1f MWh/h (=%+6.2f GWh/d)  p=%.3f\n",
              tag, spec_label, c1[1], eff_gwh_d, c1[3]))
  data.frame(tag=tag, spec=spec_label,
             eff_mwh_h=c1[1], eff_gwh_d_eq=eff_gwh_d,
             se=c1[2], p=c1[3], n=nobs(m), stringsAsFactors=FALSE)
}

specs <- list()
for (t in c("hydro", "hydro_pump")) {
  # Levels at both reforms
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
  # Migration
  specs[[length(specs)+1]] <- list(tag=sprintf("ID15 MIG (%s)", t),
                                    resp=sprintf("q_%s_mig_id15_mwh", t),
                                    pre="2022-01-01", post="2025-03-19", end="2025-04-27",
                                    spec="id15_pooled")
  specs[[length(specs)+1]] <- list(tag=sprintf("DA15 MIG (%s)", t),
                                    resp=sprintf("q_%s_mig_da15_mwh", t),
                                    pre="2022-01-01", post="2025-10-01", end="2025-12-31",
                                    spec="da15_peryear")
}

all_rows <- list()
for (s in specs) all_rows[[length(all_rows)+1]] <- run_ols(s$resp, s$pre, s$post, s$end, s$tag, s$spec)
out <- do.call(rbind, all_rows)
write.csv(out, file.path(repo, "results/regressions/bid/mtu15_critical_flat/ols_quantity_hydropump_only.csv"),
          row.names=FALSE)
cat("\nWrote ols_quantity_hydropump_only.csv\n")
