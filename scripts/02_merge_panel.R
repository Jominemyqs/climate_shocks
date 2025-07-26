## scripts/02_merge_panel.R
## Purpose: Merge cleaned sources into unified LAC country-year panel.

pkgs <- c("dplyr","readr","tidyr","stringr","purrr","lubridate")
new <- pkgs[!pkgs %in% installed.packages()[,"Package"]]
if(length(new)) install.packages(new)
invisible(lapply(pkgs, library, character.only = TRUE))

dir.create("data/processed", showWarnings = FALSE)
dir.create("output/descriptives", showWarnings = FALSE)

years <- 2008:2023
countries <- read_csv("data/intermediate/countries.csv", show_col_types = FALSE)
lac_iso3 <- countries$iso3

# ------------------------------------------------------------------
# 1. Load individual datasets (graceful if missing)
# ------------------------------------------------------------------
safe_read <- function(path){
  if(file.exists(path)) read_csv(path, show_col_types = FALSE) else NULL
}

emdat <- safe_read("data/intermediate/emdat_country_year.csv")
idmc  <- safe_read("data/intermediate/idmc_country_year.csv")
asyl  <- safe_read("data/intermediate/asylum_country_year.csv")
diasp <- safe_read("data/intermediate/diaspora_quick_placeholder.csv")
wb    <- safe_read("data/raw/worldbank_indicators.csv")

if(is.null(asyl)) stop("Asylum file missing — cannot proceed.")
if(is.null(wb)) stop("World Bank indicators missing — re-run setup to download.")

# ------------------------------------------------------------------
# 2. Prepare master skeleton (balanced panel)
# ------------------------------------------------------------------
panel_skel <- expand.grid(iso = lac_iso3, year = years) %>%
  as_tibble()

# ------------------------------------------------------------------
# 3. Tidy EM-DAT (rename columns for clarity)
#    Current columns: events_drought, deaths_drought, affected_drought, etc.
# ------------------------------------------------------------------
if(!is.null(emdat)){
  # Gather then reshape so hazards become a variable
  emdat_long <- emdat %>%
    rename_with(~str_replace(.x, "^events_", "n_")) %>%
    pivot_longer(-c(iso, year),
                 names_to = c("metric","hazard"),
                 names_pattern = "(n|deaths|affected)_(.*)",
                 values_to = "value") %>%
    pivot_wider(names_from = metric, values_from = value) %>%
    rename(events = n)
  
  # Wide hazard-specific performance (already mostly wide)
  emdat_wide <- emdat_long %>%
    pivot_wider(names_from = hazard,
                values_from = c(events, deaths, affected),
                values_fill = 0)
  
} else {
  emdat_wide <- panel_skel
}

# ------------------------------------------------------------------
# 4. Tidy IDMC
# ------------------------------------------------------------------
if(!is.null(idmc)){
  # Expect columns: iso, year, int_disp_drought, int_disp_flood, int_disp_hurricane
  idmc_wide <- idmc
} else {
  idmc_wide <- panel_skel
}

# ------------------------------------------------------------------
# 5. Asylum applications
# ------------------------------------------------------------------
asyl_wide <- asyl %>%
  rename(iso = origin) %>%
  mutate(year = as.integer(year))

# ------------------------------------------------------------------
# 6. World Bank indicators (simpler & robust)
wb_tidy <- wb %>%
  # WDI typically gives: iso2c, country, year, <indicators>, iso3c
  mutate(
    iso_final = dplyr::coalesce(iso3c, iso2c),  # prefer iso3c if present
    iso_final = toupper(iso_final)
  ) %>%
  select(
    iso = iso_final,
    year,
    gdp_pc_const,
    agri_va_pct,
    unemp_rate,
    remit_gdp_pct,
    population,
    gini_index
  ) %>%
  filter(iso %in% lac_iso3, year %in% years)


# ------------------------------------------------------------------
# ------------------------------------------------------------------
# 7. Diaspora
# ------------------------------------------------------------------
if(!is.null(diasp)){
  diaspora_wide <- diasp %>%
    rename(iso = iso3) %>%              # <-- key rename
    mutate(year = as.integer(year))
} else {
  diaspora_wide <- panel_skel %>%
    mutate(diaspora_US = NA_real_, diaspora_ESP = NA_real_)
}

# ------------------------------------------------------------------
# 8. Merge all (left join from balanced skeleton)
# ------------------------------------------------------------------
panel <- panel_skel %>%
  left_join(emdat_wide, by = c("iso","year")) %>%
  left_join(idmc_wide,  by = c("iso","year")) %>%
  left_join(asyl_wide,  by = c("iso","year")) %>%
  left_join(wb_tidy,    by = c("iso","year")) %>%
  left_join(diaspora_wide, by = c("iso","year"))



# Replace missing event/displacement counts with 0
count_cols <- grep("^(events_|deaths_|affected_|int_disp_)", names(panel), value=TRUE)
panel <- panel %>%
  mutate(across(all_of(count_cols), ~ replace_na(.x, 0)))

# ------------------------------------------------------------------
# 9. Derived variables
# ------------------------------------------------------------------
panel <- panel %>%
  mutate(
    total_events = coalesce(events_drought,0)+coalesce(events_flood,0)+coalesce(events_hurricane,0),
    total_int_disp = rowSums(across(starts_with("int_disp_")), na.rm=TRUE),
    asylum_apps_pc100k = if_else(!is.na(population) & population>0,
                                 1e5 * asylum_apps / population, NA_real_),
    int_disp_total_pc100k = if_else(!is.na(population) & population>0,
                                    1e5 * total_int_disp / population, NA_real_),
    deaths_total = rowSums(across(starts_with("deaths_")), na.rm=TRUE),
    affected_total = rowSums(across(starts_with("affected_")), na.rm=TRUE),
    # Intensities per event (safe division)
    deaths_per_event = if_else(total_events>0, deaths_total/total_events, NA_real_),
    affected_per_event = if_else(total_events>0, affected_total/total_events, NA_real_),
    any_drought_event = as.integer(coalesce(events_drought,0) > 0),
    any_flood_event = as.integer(coalesce(events_flood,0) > 0),
    any_hurricane_event = as.integer(coalesce(events_hurricane,0) > 0),
    any_internal_disp = as.integer(total_int_disp > 0),
    log_asylum_apps = log1p(asylum_apps),
    log_int_disp_total = log1p(total_int_disp),
    log_events_total = log1p(total_events)
  )

# ------------------------------------------------------------------
# 10. Lag variables (1-year)
# ------------------------------------------------------------------
panel <- panel %>%
  group_by(iso) %>%
  arrange(year) %>%
  mutate(
    lag_asylum_apps = dplyr::lag(asylum_apps, 1),
    lag_int_disp_total = dplyr::lag(total_int_disp, 1),
    lag_total_events = dplyr::lag(total_events, 1),
    lag_drought_events = dplyr::lag(events_drought, 1),
    lag_flood_events = dplyr::lag(events_flood, 1),
    lag_hurricane_events = dplyr::lag(events_hurricane, 1)
  ) %>%
  ungroup()

# ------------------------------------------------------------------
# 11. Long format (hazard decomposition) for some analyses
# ------------------------------------------------------------------
haz_long <- panel %>%
  select(iso, year,
         starts_with("events_"),
         starts_with("deaths_"),
         starts_with("affected_"),
         starts_with("int_disp_")) %>%
  pivot_longer(-c(iso,year),
               names_to = c("metric","hazard"),
               names_pattern = "(events|deaths|affected|int_disp)_(.*)",
               values_to = "value") %>%
  mutate(type = case_when(
    metric == "events" ~ "event_count",
    metric == "deaths" ~ "fatalities",
    metric == "affected" ~ "affected_pop",
    metric == "int_disp" ~ "internal_displacements"
  ))

# ------------------------------------------------------------------
# 12. Missingness summary
# ------------------------------------------------------------------
miss_summary <- panel %>%
  summarise(across(everything(), ~ sum(is.na(.)))) %>%
  pivot_longer(everything(), names_to="variable", values_to="n_missing") %>%
  arrange(desc(n_missing))

write_csv(panel, "data/processed/panel_merged_wide.csv")
write_csv(haz_long, "data/processed/panel_merged_long.csv")
write_csv(miss_summary, "output/descriptives/variable_availability.csv")

message("Merged panel written: ",
        "panel_merged_wide.csv (", nrow(panel), " rows; ",
        n_distinct(panel$iso), " countries × ", length(years), " years).")

# Quick console summary
print(
  panel %>%
    summarise(
      countries = n_distinct(iso),
      year_min = min(year),
      year_max = max(year),
      mean_asylum = mean(asylum_apps, na.rm=TRUE),
      mean_events = mean(total_events, na.rm=TRUE)
    )
)

message("Done. Next: create descriptive tables / plots, refine hurricane classification, add drought spell indicators if needed.")
