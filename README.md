# boring_cities
Code for cleaning business location, permit, and parcel data for use in Boring Cities project

## name parser
contains functions that clean, classify and parse names using regular expressions and probabalistic parsing using census name data

## address parser
contains functions that clean and parse addresses using regular expressions

## clean business data
takes in business location data and cleans and standardizes it

## create business variables
takes business location data and turns it into panel data. It also creates misc variables like if a business is publically traded 
and how many other locations exist in a given year

## clean parcel data
INCOMPLETE, but has skeleton needed for cleaning parcel data. Is a to-do as I figure out how I want to integrate parcel data.

## helper functions
contains useful functions like logging that are used in all scripts

## data constants
contains things like file paths. done in data constants so that code is easily transferable between computers. 
