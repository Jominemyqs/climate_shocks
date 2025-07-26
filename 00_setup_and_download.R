## scripts/00_setup_and_download.R
## Purpose: Initialize repo structure, define backbone panel, download API-available data,
##          and create/update raw data provenance log.

# ----------------------------- 0. Packages -----------------------------------
needed <- c("WDI","dplyr","readr","countrycode","stringr","tibble")
new <- needed[!needed %in% installed.packages()[,"Package"]]
if(length(new)) install.packages(new)

library(WDI)
library(dplyr)
library(readr)
library(countrycode)
library(stringr)
library(tibble)

# ----------------------------- 1. Directory scaffold --------------------------
dirs <- c("data/raw","data/intermediate","data/processed",
          "scripts","output/descriptives","docs","R")
invisible(lapply(dirs, dir.create, recursive = TRUE, showWarnings = FALSE))
file.create("data/raw/.gitkeep")

# ----------------------------- 2. Country list --------------------------------
lac_iso3 <- c(
  "ARG","BOL","BRA","CHL","COL","ECU","GUY","PRY","PER","SUR","URY","VEN",  # South America
  "BLZ","CRI","SLV","GTM","HND","NIC","PAN","MEX",                         # Central America + Mexico
  "CUB","DOM","HTI","JAM","TTO","BHS","BRB","GRD","LCA","VCT","ATG","KNA"  # Caribbean
)

countries <- tibble(
  iso3 = lac_iso3,
  country = countrycode(lac_iso3, "iso3c", "country.name")
)

write_csv(countries, "data/intermediate/countries.csv")

# ----------------------------- 3. Time span & panel ---------------------------
years <- 2008:2023
panel_master <- expand.grid(iso3 = lac_iso3, year = years) %>% as_tibble()
write_csv(panel_master, "data/intermediate/panel_master.csv")

# ----------------------------- 4. Raw data log setup --------------------------
log_path <- "docs/raw_data_log.md"
if(!file.exists(log_path)){
  writeLines(c(
    "# Raw Data Log",
    "",
    "| File | Source | Download_Date | Notes |",
    "|------|--------|---------------|-------|"
  ), log_path)
}

append_log <- function(file, source, notes=""){
  stopifnot(file.exists(file))
  today <- as.character(Sys.Date())
  line <- paste0("| ", basename(file), " | ", source, " | ", today, " | ", notes, " |")
  # Avoid duplicate lines
  existing <- readLines(log_path)
  if(!any(grepl(basename(file), existing))) {
    write(line, file = log_path, append = TRUE)
  } else {
    message("Log already contains entry for ", basename(file))
  }
}

# ----------------------------- 5. World Bank indicators -----------------------
# Install & load WDI if needed
if(!requireNamespace("WDI", quietly = TRUE)) install.packages("WDI")
library(WDI)

# LAC ISO3 list (reuse existing file)
lac_iso3 <- readr::read_csv("data/intermediate/countries.csv", show_col_types = FALSE)$iso3
years <- 2008:2023

wb_indicators <- c(
  gdp_pc_const = "NY.GDP.PCAP.KD",
  agri_va_pct  = "NV.AGR.TOTL.ZS",
  unemp_rate   = "SL.UEM.TOTL.ZS",
  remit_gdp_pct= "BX.TRF.PWKR.DT.GD.ZS",
  population   = "SP.POP.TOTL",
  gini_index   = "SI.POV.GINI"
)

wb_raw <- WDI(country = lac_iso3,
              indicator = wb_indicators,
              start = min(years),
              end   = max(years),
              extra = FALSE)  # no extra cols needed

# Make sure directory exists
dir.create("data/raw", showWarnings = FALSE)

readr::write_csv(wb_raw, "data/raw/worldbank_indicators.csv")
message("Saved World Bank indicators to data/raw/worldbank_indicators.csv (rows: ", nrow(wb_raw), ")")


# ----------------------------- 6. Helper: manual file logging -----------------
# Use this function AFTER you manually download & place each file into data/raw/.
log_manual_file <- function(filename, source, notes=""){
  path <- file.path("data/raw", filename)
  if(!file.exists(path)){
    message("File not found at ", path, ". Place it there first.")
  } else {
    append_log(path, source, notes)
    message("Logged: ", filename)
  }
}

# ----------------------------- 7. Manual download instructions ----------------
#cat("\n========== MANUAL DOWNLOAD TODO LIST ==========\n")
#cat("1. EM-DAT events (2008-2023) for listed countries; types: Drought, Flood, Storm.\n")
#cat("   Save as: data/raw/emdat_2008_2023.csv\n")
#cat("2. IDMC disaster new displacements (2008-2023): save as idmc_disaster_displacements.csv\n")
#cat("3. UNHCR asylum applications by origin (annual): save as unhcr_asylum_origin.csv\n")
#cat("4. UN DESA migrant stock (matrix or subset, incl. destinations US & Spain): un_desa_migrant_stock.csv\n")
#cat("After placing each file in data/raw/, run log_manual_file('filename.csv', 'Source Name', 'Any filter notes').\n")
#cat("================================================\n\n")

# ----------------------------- 8. Example logging calls (comment until ready) -
# Uncomment & run once the raw files are in place:
log_manual_file("emdat_2008_2023.csv", "EM-DAT", "Drought/Flood/Storm; 2008-2023; LAC selection")
log_manual_file("idmc_disaster_displacements.csv", "IDMC", "New displacements due to disasters 2008-2023")
log_manual_file("unhcr_asylum_origin.csv", "UNHCR", "Annual asylum applications all destinations")
log_manual_file("un_desa_migrant_stock.csv", "UN DESA", "Migrant stock; US + Spain extraction")

# ----------------------------- 9. Quick integrity checks ----------------------
# (Safe even if manual files not yet there; it will just warn.)
safe_read <- function(path){
  if(file.exists(path)) read_csv(path, show_col_types = FALSE) else NULL
}

emdat_try  <- safe_read("data/raw/emdat_2008_2023.csv")
if(!is.null(emdat_try)){
  if("ISO" %in% names(emdat_try)){
    extra_iso <- setdiff(unique(emdat_try$ISO), lac_iso3)
    if(length(extra_iso)) message("EM-DAT contains extra ISO codes: ", paste(extra_iso, collapse=", "))
  }
}

asylum_try <- safe_read("data/raw/unhcr_asylum_origin.csv")
if(!is.null(asylum_try)){
  # Try to detect ISO column heuristically
  iso_col <- intersect(names(asylum_try), c("OriginISO","ISO3","iso3","iso_code"))
  if(length(iso_col)){
    extra_iso2 <- setdiff(unique(asylum_try[[iso_col[1]]]), lac_iso3)
    if(length(extra_iso2)) message("UNHCR file contains extra ISO codes: ", paste(extra_iso2, collapse=", "))
  }
}

message("Setup script completed. Check docs/raw_data_log.md for the updated provenance table.")
