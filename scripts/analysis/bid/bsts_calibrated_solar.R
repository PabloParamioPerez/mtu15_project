# Calibrated-coefficient counterfactual: predict the ID15 post-window using
# the SOLAR coefficient identified from the DA15 spec (which sees full 2024
# and Jan-Sep 2025, i.e., much more 2025 data than the ID15 spec). This
# tests how much of the apparent ID15 price drop is solar's effect vs the
# reform's effect.

suppressPackageStartupMessages({ library(arrow); library(bsts); library(zoo) })
repo <- "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
panel <- read_parquet(file.path(repo, "data/derived/panels/bsts_quantities_panel.parquet"))
panel$d <- as.Date(panel$d)
panel <- panel[order(panel$d), ]

run_bsts_with_calibrated_solar <- function(response, pre_start, post_start, post_end,
                                            solar_coef_2024p, wind_coef, niter=3000) {
  pre_lo <- as.Date(pre_start); cutover <- as.Date(post_start); pe <- as.Date(post_end)
  sub <- panel[panel$d >= pre_lo & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, "wind_gwh", "solar_gwh", "gas_eur")]), ]
  sub$y2024p <- as.integer(as.integer(format(sub$d, "%Y")) >= 2024)
  n <- nrow(sub); post_mask <- sub$d >= cutover; n_post <- sum(post_mask)

  # Residualize: subtract the calibrated solar+wind contribution for 2024+
  # leaving the remaining variation to be fit by BSTS state + base controls
  y <- sub[[response]]
  y_resid <- y - solar_coef_2024p * sub$solar_gwh * sub$y2024p -
             wind_coef * sub$wind_gwh * sub$y2024p
  y_resid_train <- y_resid; y_resid_train[post_mask] <- NA

  X <- as.matrix(sub[, c("wind_gwh", "solar_gwh", "gas_eur")])
  ss <- AddLocalLevel(list(), y_resid_train, sigma.prior=SdPrior(sigma.guess=0.01))
  ss <- AddSeasonal(ss, y_resid_train, nseasons=7, season.duration=1)
  data_df <- data.frame(y=y_resid_train, X)
  fmla <- y ~ wind_gwh + solar_gwh + gas_eur
  set.seed(42)
  model <- bsts(fmla, state.specification=ss, niter=niter,
                 data=data_df, ping=0, seed=42)
  burn <- SuggestBurn(0.1, model)
  preds <- predict(model, newdata=as.data.frame(X[post_mask, , drop=FALSE]),
                    horizon=n_post, burn=burn, quantiles=c(.025,.975))
  cf_resid <- preds$mean

  # Add back the calibrated solar/wind contribution in the post-window to get full CF
  add_back <- solar_coef_2024p * sub$solar_gwh[post_mask] +
              wind_coef * sub$wind_gwh[post_mask]
  cf_full <- cf_resid + add_back

  observed <- y[post_mask]
  eff <- mean(observed - cf_full)

  # CI via draws
  draws <- t(preds$distribution)
  draws_full <- draws + matrix(add_back, ncol=ncol(draws), nrow=length(add_back))
  draws_eff <- observed - draws_full
  avg_draws <- colMeans(draws_eff)
  ci_lo <- quantile(avg_draws, .025); ci_hi <- quantile(avg_draws, .975)

  list(eff=eff, ci_lo=as.numeric(ci_lo), ci_hi=as.numeric(ci_hi),
       n_post=n_post, mean_solar_post=mean(sub$solar_gwh[post_mask]))
}

# Solar coefficients to test (each is the "calibrated true" 2024+ effect):
solar_coefs <- list(
  list(name="0.21 (BSTS uncond default)",      coef=-0.21),
  list(name="0.29 (per-year cond 2025, ID15)", coef=-0.29),
  list(name="0.40 (per-year cond 2024, DA15-DA)",coef=-0.40),
  list(name="0.46 (per-year cond 2024, ID15)", coef=-0.46),
  list(name="0.50 (per-year cond 2025, DA15)", coef=-0.50)
)

specs <- list(
  list(tag="ID15 IDA",    response="ida_price_eur", pre_lo="2022-01-01", post_lo="2025-03-19", post_hi="2025-04-27"),
  list(tag="PLB ID15 IDA",response="ida_price_eur", pre_lo="2022-01-01", post_lo="2024-03-19", post_hi="2024-04-27"),
  list(tag="DA15 DA",     response="da_price_eur",  pre_lo="2022-01-01", post_lo="2025-10-01", post_hi="2025-12-31"),
  list(tag="PLB DA15 DA", response="da_price_eur",  pre_lo="2022-01-01", post_lo="2024-10-01", post_hi="2024-12-31")
)
WIND <- -0.50  # stable across specs

for (s in specs) {
  cat(sprintf("\n=== %s (mean post solar ≈ %s GWh/day) ===\n", s$tag, "TBD"))
  for (sc in solar_coefs) {
    r <- run_bsts_with_calibrated_solar(s$response, s$pre_lo, s$post_lo, s$post_hi,
                                         solar_coef_2024p=sc$coef, wind_coef=WIND)
    cat(sprintf("  solar 2024+ = %-32s  TE = %+7.2f  CI=[%+7.2f, %+7.2f]  (mean post solar=%.1f GWh/day)\n",
                sprintf("-%.2f", -sc$coef), r$eff, r$ci_lo, r$ci_hi, r$mean_solar_post))
  }
}
