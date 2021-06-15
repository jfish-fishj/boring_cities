# Project Description:
Boring Cities tracks the trajectory of major US cities, focusing mostly on the leadup and aftermath of the Great Recession. In particular, this project examines the distribution of businesses within cities across time. Motivating questions are:
- Are businesses increasingly located in dense, downtown neighborhoods?
- Are businesses increasingly located in wealthier, whiter neighborhoods
- The mirror of this question is “are wealthier, whiter people increasingly moving to neighborhoods with more businesses”
- Did the Great Recession have a disproportionate impact on working class neighborhoods?
- Are publicly traded and chain businesses becoming more common?
- Has the diversity of businesses within a neighborhood changed since the Great Recession?
# Data:
Data are from business license and permitting data for the following cities:
- San Diego
- Los Angeles
- Sacramento
- Seattle
- Baton Rouge
- Saint Louis
- **Philadelphia**
- **Chicago**
- **Albuquerque*
Bold cities indicate that the data are from permits and may not contain the universe of all businesses.
# Project Status:
The business data has been cleaned, geocoded, standardized, and linked with parcel records in each of the cities. For each of the cities I now have a business level panel that details each year in which the business existed with other variables such as the business’ census tract, linked parcelID, geolocation, number of businesses with the same name, and information about business tenure (when the business started, how long it has existed, etc.)

The next phase of the project will be to pare down the business data to be just “Third Space” locations, meaning restaurants, bars, social clubs, and other spaces where I would expect people to hang out. 
# Code:
## Workflow
The workflow is split into the following parts:

1. A python script called TOP_FILE.py runs the cleaning and standardizing for each city, with the generic process being:
Raw business data are read in as csvs. These data are then cleaned and standardized to meet the business data schema established in <link to file>. This process involves cleaning the business’ name, parsing and standardizing the business’ address into usable components,parsing dates, renaming and adding columns, and mapping each business’ type into standardized categories.
2. The result of this process is a flat file that lists all businesses that have existed in a city, with columns for where this business is located, what name(s) it has, who owns it, when it first started, and when (if applicable) it closed
3. Each city will have a corresponding master address file (ideally) containing all the addresses in that city or county, with lat/long coordinates and links from that address to a parcel record. These data then have their names and addresses parsed and standardized and exported to a csv.
4. The cleaned business data is read in once again and is merged against the master address file to obtain the coordinates and parcelID that each business’ address corresponds to. Typically, I will be able to link about 90-95% of businesses to a parcel record.
5. The business file with the merged addresses is then spatially joined against a census block group shapefile in order to get the census block and tract that that business resides in.
6. This business file is then appended to a SQL database called business_locations_flat
7. This business file is converted from flat to long format. So a business that started in 2008 and ended in 2015 has 8 rows. Here I create variables that check how many businesses shared a name with that business in that year. So if there were 113 Starbucks in 2013 in Chicago, each business would have 113 for that column.
8. These data are then appended to a SQL database called business_locations_panel
9. An r script called business_descriptives.R reads in data from the business_locations_panel database and joins on census characteristics as needed.
The script then makes various data aggregations, usually at the year or tract-year level. 
10. These aggregations are saved and analysis is performed on them.
## Descriptions of files:
### Python Modules
**TOP_FILE.py** runs the full cleaning and standardizing process for each metro
#### Helper Files:
These are files that contain functions that are called by other scripts but that do not actually create any output
- **Address_parsing.py** contains the functions needed to parse and standardize addresses
- **Name_parsing.py** contains the functions needed to parse and standardize names
- **Data_constants.py** contains things like file paths and the list of cities that are being analyzed. Done in data constants so that code is easily transferable between computers
- **Helper_functions.py** contains useful functions that are called through cleaning
- **make_business_vars.py** contains functions that are used to do things like check if a business is publicly traded.
#### Cleaning Files:
These are files that are typically wrappers around the helper files, with extra city specific code as needed. These are the files that clean and process the business, address, and parel data.
- **Clean_business_data.py** takes in raw business data and cleans and standardizes it
- **Clean_address_data.py** takes in raw address data and cleans and standardizes it
- **Merge_address_data.py** merges address data onto the cleaned business data
- **Merge_census_data.py** merges the census tract and Block ID onto the cleaned business data

### R Scripts
- **business_descriptives.R** Reads in business panel data and performs various data aggregations and analysis
- **make_la_panel.R** is equivalent to making the panel in python but the LA file is huge and R is way more memory efficient than Pandas.







