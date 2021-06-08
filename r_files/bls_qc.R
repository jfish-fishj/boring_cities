library(zipcodeR)
bls = fread("bls/appended_zipcode/appended_zip.csv", colClasses = c("zip"="character"))
bls = bls %>% merge(zipcodeR::zip_code_db, by.x="zip", by.y="zipcode")
bls_agg = bls[,list(bls_est=sum(est,na.rm=T)), by = .(zip,naics, year, major_city, county, state)]
#View(bls_agg[order(-bls_est)])
bls_agg[,min_year := min(year), by = zip]
bls_agg[order(year, zip,naics),lag_businesses :=  shift(bls_est, type="lag"), by = .(zip, naics)]
bls_agg[order(year, zip,naics),change_businesses := replace_na(bls_est -lag_businesses,0), by = .(zip,naics)]
bls_agg[order(year, zip, naics), cum_change_businesses := cumsum(replace_na(change_businesses,0)), by = .(zip,naics)]
#View(bls_agg[zip %in% unique(bls_agg[order(-abs(bls_est))], by = "zip")[1:100,zip]])

city_agg = bls_agg[,list(bls_est = sum(bls_est),
                         change_businesses = sum(change_businesses),
                         cum_change_businesses = sum(cum_change_businesses)),
                   by = .(major_city, year,naics)]

top_ten_cities = (city_agg[
                             
                             major_city %in%
                             unique(city_agg[, list(cum_change_businesses = sum(cum_change_businesses)), by = .(major_city, year)][
                               order((cum_change_businesses))],
                               by ="major_city")[
                                 1:10,major_city]][,.SD[max(abs(cum_change_businesses))>100], by =.(naics, major_city)])
ggplot(data = top_ten_cities[year > 1999], aes(x=year, y= bls_est, group = naics, color = naics)) + 
  geom_line() + scale_y_continuous(labels = scales::comma) +
  facet_wrap(~major_city, ncol=3)

valid_zips = bp %>% 
  count(source,parsed_addr_zip, year)  %>%
  pull(parsed_addr_zip) %>% str_remove_all("_") %>% as.numeric() %>% unique()
bls_filter = bls[zip %in% valid_zips]
bls_agg = bls_filter[,list(bls_est=sum(est,na.rm=T)), by = .(zip, year)]
bls_agg <- bls_agg %>% merge(
  bp %>% mutate(parsed_addr_zip = (str_remove_all(parsed_addr_zip,"_")))
  %>% count(source,parsed_addr_zip, year) ,
  by.x=c("zip","year"), by.y=c("parsed_addr_zip","year")
)
table(bls_agg$source)
bls_source_agg = bls_agg[n > 100,list(bls_est = sum(bls_est), 
                                      tax_est = sum(n)), 
                         by= .(source, year)]
ggplot(bls_source_agg[year > 2005], aes(x=year, group=1))+
  geom_line(aes(y= bls_est, color ="bls est"))+
  geom_line(aes(y=tax_est, color = "tax est")) +
  facet_grid(~source)

bls17 = bls17[zip %in% valid_zips & str_detect(naics,"^-+$")]
bls17[,sum(est - as.numeric(`n<5`))]
ggplot(bp %>% filter( year > 2005 &
                     !((ownership_type %in% c("SOLE", "TRUST", "PARTNER", "PARTNR") & is_business == "person" ) |
                                  business_type %in% c("COMMERCIAL RENTAL", "RESIDENTIAL RENTAL",
                                                       "HANDYMAN", "JANITORIAL","Apartment House",
                                                       "Commercial/Industrial Space  Rental","Business Office",
                                                       "HOUSE CLEANING", "LANDSCAPING")) & source == "san diego") %>% count(year)
) + geom_line(aes(x=year, y =n, group = 1))
table(bp$location_end_year)
