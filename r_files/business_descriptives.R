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
b_flat <- tbl(bloc_con, "business_locations_panel")
census_con <-  dbConnect(RSQLite::SQLite(), "census.db")
census = tbl(census_con, "acs") %>%
  select(GEOID10,year,blk, wht, asn, hsp, pop, medhhinc, pac, imm, oth, two) %>%
  collect()
  
city = c("sacramento", "san diego")
years = seq(2000,2018)
bp <- b_flat %>%
  filter(
    year %in% years & source %in% city
  ) %>%
  select(
    contains("date"),
    contains("year"),
    matches("cleaned.+name"),
    contains("num_loc"),
    contains("publically_traded"),
    contains("city"),
    contains("ID"),
    contains("type"),
    source
    ) %>%
  collect()

if (nrow(bp) > 1e7){
  bp <- lazy_dt(bp)
}

bp = bp %>% group_by(location_id) %>% 
  arrange(year, .by_group = TRUE) %>%
  mutate(
    first_year_at_location = min(location_start_year, na.rm = T),
    last_year_at_location = max(replace_na(location_end_year, 2020)),
    years_running =(year-first_year_at_location) + seq(n())
  ) %>% ungroup() %>%
  left_join(
    census, by = c("CT_ID_10"="GEOID10", "year"="year")
  )

sf = read_sf(paste0(file_prefix,"boring_cities/data/census/shapefiles/tl_2010_06_bg10/tl_2010_06_bg10.shp"))

# check proportion geocoded
# table(bp$merged_from) # geocode rate about 95%
sum(is.na(bp$pop)) / nrow(bp)

# year descriptives
year_agg <- bp  %>%
  group_by( year, source ) %>%
  summarise(
    # business vars
    num_locations = n(),
    num_businesses = n_distinct(business_id),
    num_is_publically_traded = sum(
      (cleaned_dba_name_is_publically_traded == "is publically traded") |
        (cleaned_business_name_is_publically_traded == "is publically traded") |
        (cleaned_ownership_name_is_publically_traded == "is publically traded"), na.rm = T),
    num_chain_dba_business = sum(
      (num_locations_cleaned_business_name > 1) |(num_locations_cleaned_dba_name > 1) |(num_locations_business_id > 1),
      na.rm = T),
    num_chain_owner = sum((num_locations_cleaned_ownership_name > 1), na.rm = T),
    num_new_locations = sum(first_year_at_location==year),
    num_closed_locations = sum(last_year_at_location==year),
    percent_chain_dba_locations = num_chain_dba_business  / num_locations,
    percent_chain_owner_locations = num_chain_owner / num_locations,
    # demographic vars
    med_pop = median(pop, na.rm = T),
    medhhinc = median(medhhinc, na.rm = T),
    med_pct_black = median(blk / pop, na.rm = T),
    med_pct_white = median(wht / pop, na.rm = T),
    med_pct_asian = median(asn / pop, na.rm = T),
    med_pct_hisp = median(hsp / pop, na.rm = T),
    med_pct_other = median(oth / pop, na.rm = T),
    med_pct_pac = median(pac / pop, na.rm = T),
    med_pct_imm = median(imm / pop, na.rm = T),
    med_pct_two = median(two / pop, na.rm = T),
    # time vars
    modal_first_year = get_mode(first_year_at_location),
    modal_last_year = get_mode(last_year_at_location),
    median_first_year = median(first_year_at_location),
    median_last_year = median(last_year_at_location),
    modal_years_existed = get_mode(years_running),
    median_years_existed = median(years_running)
  ) %>% as_tibble()

# census tract descriptives 
bp %>% filter(!is.na(pop)) %>% 
  group_by(year,source, CT_ID_10) %>%
  summarise(
    # business vars
    num_locations = n(),
    num_businesses = n_distinct(business_id),
    num_is_publically_traded = sum(
      (cleaned_dba_name_is_publically_traded == "is publically traded") |
        (cleaned_business_name_is_publically_traded == "is publically traded") |
        (cleaned_ownership_name_is_publically_traded == "is publically traded"), na.rm = T),
    num_chain_dba_business = sum(
      (num_locations_cleaned_business_name > 1) |(num_locations_cleaned_dba_name > 1) |(num_locations_business_id > 1),
      na.rm = T),
    num_chain_owner = sum((num_locations_cleaned_ownership_name > 1), na.rm = T),
    num_new_locations = sum(first_year_at_location==year),
    num_closed_locations = sum(last_year_at_location==year),
    percent_chain_dba_locations = num_chain_dba_business  / num_locations,
    percent_chain_owner_locations = num_chain_owner / num_locations,
    # demographic vars
    med_pop = median(pop, na.rm = T),
    medhhinc = median(medhhinc, na.rm = T),
    med_pct_black = median(blk / pop, na.rm = T),
    med_pct_white = median(wht / pop, na.rm = T),
    med_pct_asian = median(asn / pop, na.rm = T),
    med_pct_hisp = median(hsp / pop, na.rm = T),
    med_pct_other = median(oth / pop, na.rm = T),
    med_pct_pac = median(pac / pop, na.rm = T),
    med_pct_imm = median(imm / pop, na.rm = T),
    med_pct_two = median(two / pop, na.rm = T),
    # time vars
    modal_first_year = get_mode(first_year_at_location),
    modal_last_year = get_mode(last_year_at_location),
    median_first_year = median(first_year_at_location),
    median_last_year = median(last_year_at_location),
    modal_years_existed = get_mode(years_running),
    median_years_existed = median(years_running),
    business_per_cap = num_businesses / pop,
    more_5_business_per_cap = ifelse(business_per_cap >= 0.022,1 ,0),
    .groups = "drop_last"
) %>% arrange(desc(num_businesses),.by_group = T) %>%
  mutate(
    num_businesses_percentile = ntile(num_businesses, 100),
    cumsum_businesses = cumsum(num_businesses),
    percent_total_businesses = cumsum_businesses / sum(num_businesses)
  ) %>% ungroup() %>%
  group_by(CT_ID_10) %>%
  arrange(year) %>%
  mutate(
    max_businesses = max(num_businesses),
    min_income = min(medhhinc),
    change_businesses = num_businesses - lag(num_businesses),
    change_businesses = replace_na(change_businesses ,0),
    cum_change_businesses = cumsum(change_businesses)
) %>% ungroup() %>%
  group_by(year) %>%
  mutate(
    income_decile = ntile(medhhinc, 10),
    pop_decile = ntile(med_pop, 10),
    chain_decile = ntile(percent_chain_dba_locations, 10),
    
  ) %>% ungroup() -> ct_agg

ct_agg_year <- ct_agg %>% group_by(year, source) %>% 
  summarise(across(matches("chain|pct|pop|first_year|existed|decile"),
                   ~ weighted.mean(.x,w=num_businesses, na.rm = T)))
View(ct_agg_year)

ggplot(data=ct_agg_year, aes(x=year, y=median_years_existed,
                             group=as.factor(source), colour=source)) + 
         geom_line()

# inner join w/ shapefile to get map 
# aggregate sf up to tract
temp<- sf %>% 
  st_transform(4326) %>%
  st_set_crs(4326) %>%
  select(GEOID10, geometry) %>%
  mutate(
  tract_id = str_sub(GEOID10, 1, 11),
  # area = st_area(geometry)
)  %>%
  group_by(tract_id) %>%
  slice(
    1
  #area = sum(area)
)

sf_ct_d <- ct_agg %>% 
  mutate(tract_id = str_pad(as.character(CT_ID_10), 11, "left", "0")) %>%
  inner_join(sf_ct %>% select(tract_id, area, geometry), by = c("tract_id" = "tract_id")) %>% st_as_sf()

cowplot::plot_grid(
  sf_ct_d %>% filter(year == 2005 ) %>% 
  ggplot() + 
  geom_sf(aes(fill = num_businesses)), 
  sf_ct_d %>% filter(year == 2010 ) %>% 
    ggplot() + 
    geom_sf(aes(fill = num_businesses)),
  sf_ct_d %>% filter(year == 2018) %>% 
    ggplot() + 
    geom_sf(aes(fill = num_businesses))
)

ggplot(sf_ct_d %>% filter(year == 2018 ) %>%
         mutate(business_cut = as.factor(cut(cum_change_businesses,
                                             breaks = c(min(cum_change_businesses, na.rm = T),-200,-100,-50,0,100,200,300, 
                                                        max(cum_change_businesses, na.rm = T))))), 
       aes(fill = business_cut)) + 
  geom_sf(lwd = 0) 



