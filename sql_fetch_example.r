# Environment:
# tradelab.mit.edu
#
# Input:
# - <country>
# - joined_v1:
#     SQL table containing joined tariff data

## ----------------------------------------------------------------------------
## Preamble / globals
## ----------------------------------------------------------------------------
rm(list=ls()) # clears objects from the workspace

library(dplyr)
library(dbplyr) # Separated from dplyr 0.6.0
library(tidyr)
library(purrr)
library(zoo)
library(RMySQL)

# set file path
#WD_PATH <- file.path("~/Downloads/")
## WD_PATH <- Sys.getenv()[["WD_PATH"]]
WD_PATH <- file.path("~/Downloads/")



## ----------------------------------------------------------------------------
## DB
## ----------------------------------------------------------------------------

uid <- "root"
pwd <- "tariff"

connect_db <- function(uid, pwd) {
  src_mysql("ra_tariff",
            host = "tradelab.mit.edu",
            port = 3306,
            user = uid,
            password = pwd)
}

db <- connect_db(uid, pwd)


#####################################################################
## list of data tables in tradelab
#####################################################################


## mysql> show tables;
## +--------------------------------+
## | Tables_in_ra_tariff            |
## +--------------------------------+
## | HS2                            |
## | HS2_deconflicted               |
## | HS4                            |
## | HS4_deconflicted               |
## | HS6                            |
## | HS6_interim                    |
## | S1                             |
## | SITCS1_1digit                  |
## | SITCS1_2digit                  |
## | SITCS1_2digit_deconflicted     |
## | SITCS1_3digit                  |
## | SITCS1_4digit                  |
## | SITCS1_5digit                  |
## | country                        |
## | country_codebook               |
## | country_map                    |
## | country_year                   |
## | country_year_codebook          |
## | country_year_industry_measures |
## | country_year_wits_section_rca  |
## | crosswalk                      |
## | dyad                           |
## | dyad_codebook                  |
## | dyad_year                      |
## | dyad_year_alliances            |
## | dyad_year_alliances_codebook   |
## | dyad_year_codebook             |
## | herfindahl_product_measures    |
## | joined                         |
## | joined_HS0                     |
## | joined_HS0_nonbound            |
## | joined_HS0_truncated           |
## | joined_HS2                     |
## | joined_HS2_truncated           |
## | joined_HS4                     |
## | joined_HS6                     |
## | joined_MFN                     |
## | joined_MFN_HS4                 |
## | joined_MFN_HS6                 |
## | joined_MFN_truncated           |
## | joined_codebook                |
## | joined_v1                      |
## | joined_v1_codebook             |
## | joined_v1_flags                |
## | nation                         |
## | national_desc                  |
## | polity_industry_measures       |
## | polity_year_industry_measures  |
## | product_h0                     |
## | product_h1                     |
## | product_h2                     |
## | product_h3                     |
## | product_h4                     |
## | product_hs                     |
## | product_hs6                    |
## | product_sitcs1                 |
## | region                         |
## | region_map_expanded            |
## | rta                            |
## | rta_expanded                   |
## | tao_duty                       |
## | tao_duty_ec2                   |
## | tao_duty_type                  |
## | taomap                         |
## | trfmeasures                    |
## | wits_duty                      |
## | wits_duty_ec2                  |
## | wits_duty_type                 |
## | year                           |
## +--------------------------------+
## 69 rows in set (0.00 sec)

    
#####################################################################
## tariffs data at the dyad-level
#####################################################################

## mysql> select * from joined_v1_codebook;
## +--------------------------+---------------------------------------------------------------------------------------------------------------------+
## | col_name                 | col_desc                                                                                                            |
## +--------------------------+---------------------------------------------------------------------------------------------------------------------+
## | partner                  | ISO code for trading partner (exporter)                                                                             |
## | code                     | HS tariff-line product code                                                                                         |
## | wits_duty_advalorem      | WITS (TRAINS) reported ad valorem rate (or ad valorem equivalent)                                                   |
## | wits_using_ave           | Whether or not the WITS (TRAINS) reported rate is an imputed ad valorem equivalent (AVE)                            |
## | wits_duty_specific       | WITS (TRAINS) reported original specific rate (if exists)                                                           |
## | wits_duty_specific_val   | WITS (TRAINS) reported parsed numerical specific rate (if exists)                                                   |
## | wits_duty_type           | WITS (TRAINS) reported rate duty type                                                                               |
## | wits_duty_desc           | Description of WITS (TRAINS) reported rate's duty type                                                              |
## | wits_is_nonpref          | Whether the WITS (TRAINS) reported rate is non-preferential (e.g. MFN or General)                                   |
## | wits_region_desc         | Description of WITS (TRAINS) duty type's regional beneficiary group (if exists)                                     |
## | wits_region_code         | Code for WITS (TRAINS) duty type's regional beneficiary group (if exists)                                           |
## | wits_flag                | Flag indicating how the WITS duty choice (if exists) was made                                                       |
## | tao_duty_advalorem       | TAO (IDB) reported ad valorem rate                                                                                  |
## | tao_duty_specific        | TAO (IDB) reported original specific rate (if exists)                                                               |
## | tao_duty_specific_val    | TAO (IDB) reported parsed numerical specific rate (if exists)                                                       |
## | tao_duty_type            | TAO (IDB) reported rate duty type                                                                                   |
## | tao_duty_desc            | Description of TAO (IDB) duty type's regional beneficiary group (if exists)                                         |
## | tao_is_nonpref           | Whether the TAO (IDB) reported rate is non-preferential (e.g. MFN or General)                                       |
## | tao_is_mixed             | Whether the TAO (IDB) reported rate is mixed (sometimes ad valorem, sometimes specific)                             |
## | tao_is_compound          | Whether the TAO (IDB) reported rate is compound (some amount ad valorem, some amount specific)                      |
## | tao_region_desc          | Description of TAO (IDB) duty type's regional beneficiary group (if exists)                                         |
## | tao_region_code          | Code for TAO (IDB) duty type's regional beneficiary group (if exists)                                               |
## | tao_flag                 | Flag indicating how the TAO duty choice (if exists) was made                                                        |
## | joined_source            | Which source (between WITS and TAO) the final duty comes from                                                       |
## | joined_duty_advalorem    | Final joined duty advalorem rate                                                                                    |
## | joined_using_ave         | Whether or not the final joined duty ad valorem rate is an imputed ad valorem equivalent                            |
## | joined_duty_specific     | Final joined duty's original specific rate (if exists)                                                              |
## | joined_duty_specific_val | Final joined duty's parsed numerical specific rate (if exists)                                                      |
## | joined_is_nonpref        | Whether the final joined duty rate is non-preferential (e.g. MFN or General)                                        |
## | joined_flag              | Flag indicating how the final duty choice was made between WITS and TAO candidates (see corresponding flags lookup) |
## | year                     | Year of enforced tariff rate on given tariff-line                                                                   |
## | nation_id                | ISO code for trading nation (importer)                                                                              |
## +--------------------------+---------------------------------------------------------------------------------------------------------------------+


## ----------------------------------------------------------------------------
## tariffs data example: tariffs data is stored in joined_v1 table
## ----------------------------------------------------------------------------

## US tariffs towards products from Korea in 2016. Looking at products
## with the first 4 digit of the HS code == 6401

## select year, nation_id as imp, partner as exp, code,
## joined_duty_advalorem, wits_duty_desc,
## joined_source,joined_is_nonpref as is_mfn from joined_v1 where
## nation_id = "USA" and partner = "KOR" and year = 2016 and code LIKE
## "6401%";


COUNTRY <- "USA"
PARTNER <- "KOR"

df1 <- NULL
df1 <- rbind(df, tbl(db, "joined_v1") %>%
            filter(!is.na(joined_duty_advalorem), nation_id == COUNTRY,
                   partner == PARTNER, year == 2016, code %regexp% "^6401[0-9]*") %>%
            collect(n=Inf))
data1 <- df1 %>% select(nation_id, partner, year, code, joined_duty_advalorem, wits_duty_desc, joined_source, joined_is_nonpref)
data1 <- as.data.frame(data1)

head(data1)


write.csv(data1,
  file.path(WD_PATH, sprintf("tariff_%s_%s.csv", COUNTRY, PARTNER)), row.names=F)



## ----------------------------------------------------------------------------
## trade volume data example
## ----------------------------------------------------------------------------

## US trade with Korea Looking at products with the first 4 digit of
##  the HS code == 6401

## rg_code encodes whether the reported value is imports or exports
## from the reporter's perspective. So for the trade between countries
## A and B *and* if the data is from country A, rg_code==1 means A's
## import from B, rg_code==2 means A's export to B. 3 and 4 are
## re-exports and re-imports which you can ignore for now.
## see https://unstats.un.org/unsd/tradekb/Knowledgebase/50039/UN-Comtrade-Reference-Tables

COUNTRY <- "USA"
PARTNER <- "KOR"

df2 <- NULL
df2 <- rbind(df2, tbl(db, "HS6") %>%
             filter(reporter_iso3 == COUNTRY,
                    partner_iso3 == PARTNER, year == 2016, code %regexp% "^6401[0-9]*") %>%
             collect(n=Inf))
data2 <- as.data.frame(df2)

head(data2)


write.csv(data2,
  file.path(WD_PATH, sprintf("tariff_%s_%s.csv", COUNTRY, PARTNER)), row.names=F)

