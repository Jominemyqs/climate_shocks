## scripts/01_process_raw.R
## Purpose: Process raw datasets (EM-DAT, IDMC, UNHCR, UN DESA placeholder)
##          into tidy LAC country-year aggregates for 2008–2023.

# ========================== 0. Packages =======================================
pkgs <- c("readr","readxl","dplyr","stringr","tidyr","lubridate","purrr","zoo","janitor")
new <- pkgs[!pkgs %in% installed.packages()[,"Package"]]
if(length(new)) install.packages(new)
invisible(lapply(pkgs, library, character.only = TRUE))

`%||%` <- function(a,b) if(!is.null(a)) a else b  # small helper

# ========================== 1. Backbone =======================================
years <- 2008:2023
countries_path <- "data/intermediate/countries.csv"
stopifnot(file.exists(countries_path))
countries <- read_csv(countries_path, show_col_types = FALSE)
lac_iso3  <- countries$iso3

dir.create("data/intermediate", showWarnings = FALSE)
dir.create("docs", showWarnings = FALSE)

msg_header <- function(txt){
  cat("\n", strrep("-",70), "\n", txt, "\n", strrep("-",70), "\n", sep="")
}

# Switch for storm classification:
# option = "A" -> only storms whose event name contains hurricane/cyclone keywords
# option = "B" -> treat ALL storms as hurricanes
storm_classification_option <- "A"

# To collect summary info
summary_list <- list()

# ========================== 2. EM-DAT =========================================
msg_header("Processing EM-DAT (auto extension)")

emdat_file <- "data/raw/emdat_2008_2023.csv"  # or .xlsx if you rename later
if(!file.exists(emdat_file)){
  warning("EM-DAT file not found -> skipping EM-DAT processing.")
} else {
  
  # Helper to read EM-DAT flexibly
  read_emdat <- function(path){
    ext <- tolower(tools::file_ext(path))
    if(ext %in% c("xls","xlsx")){
      df <- readxl::read_excel(path)
    } else if(ext == "csv"){
      # Detect header line (some exports have metadata lines)
      all_lines <- readr::read_lines(path, n_max = 200)
      header_line <- which(grepl("Disaster Type", all_lines, ignore.case = TRUE))[1]
      # If header_line is not 1, skip the lines before it
      skip_n <- if(is.na(header_line)) 0 else (header_line - 1)
      df <- readr::read_csv(path, skip = skip_n, show_col_types = FALSE)
    } else {
      stop("Unsupported EM-DAT file extension: ", ext)
    }
    df
  }
  
  emdat_raw <- read_emdat(emdat_file) %>% janitor::clean_names()
  
  # QUICK sanity check of column names
  # print(names(emdat_raw))
  
  # Find year column
  year_candidates <- grep("^year$|^start_?year$", names(emdat_raw),
                          value=TRUE, ignore.case=TRUE)
  if(length(year_candidates)==0){
    stop("No usable year/start_year column. Columns containing 'year': ",
         paste(grep("year", names(emdat_raw), value=TRUE), collapse=", "))
  }
  year_col <- year_candidates[1]
  emdat_raw <- emdat_raw %>% dplyr::rename(year = !!year_col)
  
  # Possible column name variants for required fields in CSV vs XLSX
  # (some EM-DAT CSV exports use: total_deaths, no_affected;
  #  others: total_deaths_(no), affected_(no)... adjust if needed.)
  required_cols <- c("iso","disaster_type","disaster_subtype","total_deaths","no_affected")
  miss <- setdiff(required_cols, names(emdat_raw))
  if(length(miss)){
    # Try fallback patterns
    alt_total <- grep("death", names(emdat_raw), value=TRUE)
    alt_aff   <- grep("affect", names(emdat_raw), value=TRUE)
    message("Missing standard columns: ", paste(miss, collapse=", "),
            "\nDetected possible death cols: ", paste(alt_total, collapse=", "),
            "\nDetected possible affected cols: ", paste(alt_aff, collapse=", "))
    stop("Adjust required column names if necessary.")
  }
  
  emdat_tidy <- emdat_raw %>%
    dplyr::mutate(iso = toupper(iso)) %>%
    dplyr::filter(
      iso %in% lac_iso3,
      !is.na(year),
      year >= min(years), year <= max(years),
      tolower(disaster_type) %in% c("drought","flood","storm")
    ) %>%
    dplyr::mutate(
      is_hurricane = stringr::str_detect(
        tolower(`disaster_subtype` %||% ""),
        "hurricane|tropical cyclone|cyclone|typhoon"
      ),
      hazard_group = dplyr::case_when(
        tolower(disaster_type) == "drought" ~ "drought",
        tolower(disaster_type) == "flood"   ~ "flood",
        tolower(disaster_type) == "storm" & is_hurricane ~ "hurricane",
        TRUE ~ NA_character_
      ),
      deaths   = suppressWarnings(as.numeric(total_deaths)),
      affected = suppressWarnings(as.numeric(no_affected))
    ) %>%
    dplyr::filter(!is.na(hazard_group))
  
  emdat_aggr <- emdat_tidy %>%
    dplyr::group_by(iso, year, hazard_group) %>%
    dplyr::summarise(
      events   = dplyr::n(),
      deaths   = sum(deaths, na.rm=TRUE),
      affected = sum(affected, na.rm=TRUE),
      .groups="drop"
    )
  
  emdat_wide <- emdat_aggr %>%
    tidyr::pivot_wider(
      names_from = hazard_group,
      values_from = c(events, deaths, affected),
      values_fill = 0
    )
  
  readr::write_csv(emdat_wide, "data/intermediate/emdat_country_year.csv")
  message("EM-DAT processed -> emdat_country_year.csv (", nrow(emdat_wide), " rows)")
}


# ========================== 3. IDMC ==========================================
msg_header("Processing IDMC (patched minimal)")

idmc_file <- "data/raw/idmc_disaster_displacements.csv"
if(!file.exists(idmc_file)){
  warning("IDMC file not found.")
} else {
  idmc_raw <- readr::read_csv(idmc_file, show_col_types = FALSE, guess_max = 50000)
  
  # Drop ONLY the meta/header row(s) where hazard_category starts with '#crisis'
  idmc_clean <- idmc_raw %>%
    dplyr::filter(!(hazard_category %in% c("#crisis+category+code") |
                      hazard_category_name %in% c("#crisis+category+name")))  # keep real data
  
  # Quick check
  message("Rows after dropping meta rows: ", nrow(idmc_clean))
  
  idmc_tidy <- idmc_clean %>%
    dplyr::mutate(
      iso  = toupper(trimws(iso3)),
      year = readr::parse_number(year),
      hazard_type_name = trimws(hazard_type_name),
      new_disp = readr::parse_number(new_displacement)
    ) %>%
    dplyr::filter(
      iso %in% lac_iso3,
      !is.na(year), year >= 2008, year <= 2023
    ) %>%
    # Replace NA displacement with 0 (blank cells) BEFORE aggregation
    dplyr::mutate(new_disp = dplyr::coalesce(new_disp, 0)) %>%
    dplyr::mutate(
      hazard_group = dplyr::case_when(
        hazard_type_name == "Drought" ~ "drought",
        hazard_type_name == "Flood"   ~ "flood",
        hazard_type_name == "Storm"   ~ "hurricane",  # (broad for now)
        TRUE ~ NA_character_
      )
    ) %>%
    dplyr::filter(!is.na(hazard_group)) %>%
    dplyr::group_by(iso, year, hazard_group) %>%
    dplyr::summarise(new_disp = sum(new_disp, na.rm=TRUE), .groups="drop") %>%
    tidyr::pivot_wider(
      names_from = hazard_group,
      values_from = new_disp,
      values_fill = 0,
      names_prefix = "int_disp_"
    )
  
  readr::write_csv(idmc_tidy, "data/intermediate/idmc_country_year.csv")
  message("IDMC tidy written: ", nrow(idmc_tidy), " country-year rows")
  print(
    idmc_tidy %>%
      summarise(
        countries = dplyr::n_distinct(iso),
        year_min = min(year), year_max = max(year),
        total_disp = sum(int_disp_drought + int_disp_flood + int_disp_hurricane, na.rm=TRUE)
      )
  )
}


# ========================== 4. UNHCR Asylum Applications ======================
msg_header("Processing UNHCR asylum applications")

unhcr_file <- "data/raw/unhcr_asylum_origin.csv"
if(!file.exists(unhcr_file)){
  warning("UNHCR file not found -> skipping UNHCR processing.")
} else {
  asylum_raw <- read_csv(unhcr_file, show_col_types = FALSE)
  cols <- names(asylum_raw)
  
  year_col   <- intersect(cols, c("Year"))
  origin_col <- intersect(cols, c("Country of Origin ISO","OriginISO","Origin ISO","Origin Code"))
  dest_col   <- intersect(cols, c("Country of Asylum ISO","AsylumISO","Asylum ISO","Country of Asylum Code"))
  value_col  <- intersect(cols, c("Applied","Applications","Value","Asylum applications"))
  
  can_process <- length(year_col)==1 && length(origin_col)==1 && length(value_col)==1
  
  if(!can_process){
    warning("Could not detect essential UNHCR columns. Columns found: ", paste(cols, collapse=", "))
  } else {
    # Check origin dimension
    uniq_origin <- unique(asylum_raw[[origin_col]])
    if(length(uniq_origin)==1 && uniq_origin[1] %in% c("-", NA)){
      warning("UNHCR file lacks origin breakdown (all origins = '-'). Re-download with 'Country of Origin' visible. Skipping.")
    } else {
      asylum_tidy <- asylum_raw %>%
        rename(
          year   = !!year_col,
          origin = !!origin_col,
          applications = !!value_col
        ) %>%
        mutate(applications = suppressWarnings(as.numeric(applications))) %>%
        filter(
          origin %in% lac_iso3,
          year >= min(years), year <= max(years)
        ) %>%
        group_by(origin, year) %>%
        summarise(asylum_apps = sum(applications, na.rm=TRUE), .groups="drop")
      
      write_csv(asylum_tidy, "data/intermediate/asylum_country_year.csv")
      message("UNHCR processed -> data/intermediate/asylum_country_year.csv (", nrow(asylum_tidy), " rows)")
      
      # Optional destination breakdown
      if(length(dest_col)==1){
        asylum_dest <- asylum_raw %>%
          rename(
            year   = !!year_col,
            origin = !!origin_col,
            dest   = !!dest_col,
            applications = !!value_col
          ) %>%
          mutate(applications = suppressWarnings(as.numeric(applications))) %>%
          filter(
            origin %in% lac_iso3,
            year >= min(years), year <= max(years)
          ) %>%
          group_by(origin, dest, year) %>%
          summarise(asylum_apps = sum(applications, na.rm=TRUE), .groups="drop")
        write_csv(asylum_dest, "data/intermediate/asylum_origin_destination_year.csv")
        message("Destination breakdown -> asylum_origin_destination_year.csv (", nrow(asylum_dest), " rows)")
      }
      
      summary_list$unhcr <- asylum_tidy %>%
        summarise(
          countries = n_distinct(origin),
          years_min = min(year), years_max = max(year),
          total_asylum_apps = sum(asylum_apps, na.rm=TRUE)
        )
    }
  }
}

# ========================== 5. UN DESA Diaspora Placeholder ===================
msg_header("Diaspora placeholder (UN DESA)")

undesa_file <- "data/raw/un_desa_migrant_stock.xlsx"
if(!file.exists(undesa_file)){
  message("UN DESA file not found -> writing NA placeholder.")
} else {
  message("UN DESA file present (not parsed yet) -> writing NA placeholder for now.")
}

diaspora_placeholder <- expand_grid(iso3 = lac_iso3, year = years) %>%
  mutate(diaspora_US = NA_real_, diaspora_ESP = NA_real_)

write_csv(diaspora_placeholder, "data/intermediate/diaspora_quick_placeholder.csv")
summary_list$diaspora <- diaspora_placeholder %>% summarise(rows = n())

# ========================== 6. Variable Dictionary Seed =======================
msg_header("Writing variable dictionary seed")

var_dict_path <- "docs/variable_dictionary_seed.csv"
var_seed <- tibble::tribble(
  ~variable,              ~description,                                           ~source,
  "events_drought",       "Number of drought events (EM-DAT)",                    "EM-DAT",
  "events_flood",         "Number of flood events (EM-DAT)",                      "EM-DAT",
  "events_hurricane",     "Number of hurricane/tropical cyclone events (EM-DAT)", "EM-DAT",
  "deaths_drought",       "Total deaths from drought events",                     "EM-DAT",
  "deaths_flood",         "Total deaths from flood events",                       "EM-DAT",
  "deaths_hurricane",     "Total deaths from hurricane events",                   "EM-DAT",
  "affected_drought",     "People affected by drought events",                    "EM-DAT",
  "affected_flood",       "People affected by flood events",                      "EM-DAT",
  "affected_hurricane",   "People affected by hurricane events",                  "EM-DAT",
  "int_disp_drought",     "New internal displacements due to drought",            "IDMC",
  "int_disp_flood",       "New internal displacements due to flood",              "IDMC",
  "int_disp_hurricane",   "New internal displacements due to hurricanes",         "IDMC",
  "asylum_apps",          "Total asylum applications (all destinations)",         "UNHCR",
  "diaspora_US",          "Migrant stock in United States (placeholder)",         "UN DESA",
  "diaspora_ESP",         "Migrant stock in Spain (placeholder)",                 "UN DESA"
)
write_csv(var_seed, var_dict_path)
message("Variable dictionary seed written: ", var_dict_path)

# ========================== 7. Summary =======================================
msg_header("Summary of processed components")

print(summary_list)

cat("\nFiles created (if source present):\n")
files_created <- c(
  "data/intermediate/emdat_country_year.csv",
  "data/intermediate/idmc_country_year.csv",
  "data/intermediate/asylum_country_year.csv",
  "data/intermediate/asylum_origin_destination_year.csv",
  "data/intermediate/diaspora_quick_placeholder.csv",
  "docs/variable_dictionary_seed.csv"
)
print(files_created[file.exists(files_created)])

cat("\nNEXT STEPS:\n",
    "1. If UNHCR was skipped or warned: re-download with 'Country of Origin' visible and rerun.\n",
    "2. Obtain proper UN DESA bilateral migrant stock (CSV) -> replace placeholder.\n",
    "3. Decide if you keep Option A (hurricane-like only) or switch to Option B (all storms) and rerun if needed.\n",
    "4. Request merge script once asylum & (optional) diaspora data are ready.\n")

