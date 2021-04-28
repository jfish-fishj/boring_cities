# Title     : TODO
# Objective : TODO
# Created by: JoeFish
# Created on: 2/24/21

library(tidyverse)
library(dbplyr)
library(data.table)
library(sf)
library(DBI)
library(dtplyr)

if (Sys.info()['sysname']=="Linux"){
  file_prefix = "/home/jfish/project_data/"  
} else {
  file_prefix = "/Volumes/Seagate Portable Drive/"
}
get_mode <- function(x) {
  ux <- unique(na.omit(x))
  tx <- tabulate(match(x, ux))
  if(length(ux) != 1 & sum(max(tx) == tx) > 1) {
    if (is.character(ux)) return(NA_character_) else return(NA_real_)
  }
  max_tx <- tx == max(tx)
  return(ux[max_tx])
}

setwd(str_interp("${file_prefix}boring_cities/data/"))
bloc_con <- dbConnect(RSQLite::SQLite(), "business_locations.db")
b_flat <- tbl(bloc_con, "business_locations_flat")
b_panel <- tbl(bloc_con, "business_locations_panel")
bp <- b_flat %>% filter(
  source == "los angeles"
) %>% collect()

if (nrow(bp) > 1e7){
  bp <- lazy_dt(bp)
}

bp_long <- bp %>% mutate(
  numYears = replace_na(location_end_year,2020) - location_start_year + 1,
  index = row_number()
) %>% filter(
  numYears < 120 & !is.na(location_start_year) & numYears > 0
) %>%
  group_by(index) %>% 
  uncount(numYears, .remove=F) %>%
  mutate(
    seq = seq(n()),
    year = location_start_year + seq - 1
  ) %>%
  group_by(
    cleaned_business_name, year
  ) %>% mutate(
    num_locations_cleaned_business_name = n(),
    num_locations_cleaned_business_name = data.table::fifelse(
      is.na(cleaned_business_name), NA_real_, num_locations_cleaned_business_name
    )
  ) %>% 
  ungroup() %>% 
  group_by(
    cleaned_dba_name, year
  ) %>% mutate(
    num_locations_cleaned_dba_name = n(),
    num_locations_cleaned_dba_name = data.table::fifelse(
      is.na(cleaned_dba_name), NA_real_, num_locations_cleaned_dba_name
    )
  ) %>% 
  ungroup() %>% 
  mutate(
    num_locations_cleaned_ownership_name = NA_real_,
    num_locations_business_id = NA_real_
  ) %>% select(-seq) %>%
  as_tibble()

DBI::dbWriteTable(bloc_con, 
                  "business_locations_panel",
                  bp_long %>% select(-index), 
                  append=T)





