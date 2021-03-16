# Title     : TODO
# Objective : TODO
# Created by: JoeFish
# Created on: 2/24/21

library(tidyverse)
library(data.table)
library(sf)

# san diego business non panel
sd_bp = fread("/Volumes/Seagate Portable Drive/boring_cities/data/final/sd/business location/business_location_panel.csv", data.table = F)
sd_sf = read_sf("/Volumes/Seagate Portable Drive/boring_cities/data/census/shapefiles/tl_2010_06_bg10/tl_2010_06_bg10.shp")
# filter for just places in san diego
sd_bp %>% filter(primary_cleaned_city == "san diego" & year %in% seq(2000,2018))%>%
  distinct(cleaned_dba_name,cleaned_ownership_name, year, primary_cleaned_addr_n1,
           primary_cleaned_addr_sn, primary_cleaned_addr_ss, .keep_all = T)%>% 
  group_by(year, cleaned_ownership_name) %>%
  mutate(num_locs = n() ) %>%
  ungroup()-> sd_bp_f

# check proportion geocoded
table(sd_bp$merged_from) # geocode rate about 95%
sum(is.na(sd_bp$pop)) / nrow(sd_bp)



# year descriptives
sd_bp_f %>% filter(year %in% seq(2000, 2018)) %>% group_by( year, naics_descr) %>% summarise(
  num_businesses = n(),
  num_is_publically_traded = sum(dba_is_publicly_traded == "is publically traded" | business_name_is_publicly_traded == "is publically traded", na.rm = T),
  num_chain = sum(num_locations > 1, na.rm = T),
  num_chain2 = sum(num_locs > 1),
  percent_chain = num_chain  / num_businesses,
  med_pop = median(pop, na.rm = T),
  medhhinc = median(medhhinc, na.rm = T)
) -> sd_year_naics_agg

# census tract descriptives 
sd_bp %>% filter(year %in% seq(2000, 2018) & CT_ID_10 != 0) %>% group_by(
  location_id
) %>%
  mutate(
    first_year_at_location = ifelse(year == min(year, na.rm=T),1,0),
    last_year_at_location = ifelse(year == max(year, na.rm=T),1,0)
  ) %>% ungroup() %>%
  group_by(year, CT_ID_10) %>%
  summarise(
    num_businesses = n(),
    num_is_publically_traded = sum(dba_is_publicly_traded == "is publically traded" | 
                                     business_name_is_publicly_traded == "is publically traded", na.rm = T),
    num_chain = sum(num_locations > 1, na.rm = T),
    num_new_businesses = sum(first_year_at_location),
    num_closed_businesses = sum(last_year_at_location),
    percent_chain = num_chain  / num_businesses,
    pop = first(pop),
    medhhinc = first(medhhinc),
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
    pop_decile = ntile(pop, 10),
    chain_decile = ntile(percent_chain, 10),
    
  ) %>% ungroup() -> sd_ct_agg

# inner join w/ shapefile to get map 
# aggregate sf up to tract
sd_sf %>% mutate(
  tract_id = str_sub(GEOID10, 1, 11),
  area = st_area(geometry)
) %>% group_by(tract_id) %>% summarise(
  area = sum(area)
) -> sd_sf_ct

sd_ct_agg %>% 
  mutate(tract_id = str_pad(as.character(CT_ID_10), 11, "left", "0")) %>%
  inner_join(sd_sf_ct, by = c("tract_id" = "tract_id")) %>% st_as_sf() -> sd_sf_ct_d

cowplot::plot_grid(
  sd_sf_ct_d %>% filter(year == 2000 ) %>% 
  ggplot() + 
  geom_sf(aes(fill = num_businesses)), 
  sd_sf_ct_d %>% filter(year == 2010 ) %>% 
    ggplot() + 
    geom_sf(aes(fill = num_businesses)),
  sd_sf_ct_d %>% filter(year == 2018) %>% 
    ggplot() + 
    geom_sf(aes(fill = num_businesses))
)

ggplot(sd_sf_ct_d %>% filter(year == 2018 ) %>%
         mutate(business_cut = as.factor(cut(cum_change_businesses,
                                             breaks = c(min(cum_change_businesses, na.rm = T),-200,-100,-50,0,100,200,300, 
                                                        max(cum_change_businesses, na.rm = T))))), 
       aes(fill = business_cut)) + 
  geom_sf(lwd = 0) 


sd_ct_agg %>% group_by( income_decile, year) %>% filter(!is.na(income_decile)) %>% summarise(
  num_businesses = sum(num_businesses),
  num_new_businesses = sum(num_new_businesses),
  num_closed_businesses = sum(num_closed_businesses),
  num_chain = sum(num_chain),
  num_is_publically_traded = sum(num_is_publically_traded),
  num_5_business = sum(pop * more_5_business_per_cap),
  pop = sum(pop),
  .groups = "drop_last"
) %>% mutate( 
  percent_5_business = num_5_business / pop,
  change_businesses = num_businesses - lag(num_businesses),
  percent_change_businesses = change_businesses / lag(num_businesses)
) %>% View()


