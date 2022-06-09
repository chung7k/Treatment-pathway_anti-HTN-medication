library(dplyr)
library(plyr)
library(tidyverse)
library(intervals)
library(data.table)
library(lubridate)
library(timeperiodsR)

data_all <- read_csv(".csv")
data_curated  <- data_all %>% filter(days_supply >= 1)
data_fin <- ddply(data_curated, .(person_id),mutate, seq_for_drug = cumsum(concept_name != lag(concept_name, default="" )) )


write_csv(data_fin, ".csv")
