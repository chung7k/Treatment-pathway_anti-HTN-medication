library(tidyverse)
library(plyr)
library(intervals)
library(data.table)
library(lubridate)
library(timeperiodsR)
library(readr)

data_all <- readr::read_csv(".csv")
                        
split_interval <- function(start, end, unit){
  breaks <- seq(floor_date(start, unit), ceiling_date(end,unit), by = unit)
  timeline <- c(start, breaks[breaks>start & breaks < end], end)
  tibble(.start = head(timeline, -1), .end=tail(timeline, -1))
}

data_split <- data_all %>% group_by(person_id) %>% mutate(periods = map2(drug_exposure_start_date, drug_exposure_end_date, split_interval, unit="days"))
data_long <- data_split %>% unnest(periods)
data_drug <- ddply(data_long, .(person_id,.start, .end), summarise, drug_concept_id=toString(unique(sort(drug_concept_id))), concept_name=toString(unique(sort(concept_name))))
data_drug2 <- as.data.frame(data_drug$drug_concept_id)
data_drug3 <- cbind(data_drug, data_drug2)


names(data_drug3)[4] <- c("group")
data_drug3$group <- as.numeric(as.factor(data_drug3$group))
data_drug3 <- unique(data_drug3)



data_drug3$sep <- with(rle(data_drug3$group), rep(values*cumsum(values & lengths>=2), lengths))
data_drug3$sep <- as.numeric(data_drug3$sep)
data_drug3$gap <- c(0, !diff(data_drug3$sep)==0|diff(data_drug3$.end)>1)
data_drug3$group <- cumsum(data_drug3$gap)+1
names(data_drug3)[6] <-c("drug_concept_id") 
data_drug3 <- subset(data_drug3, select=-gap)

data_drug3 <- data_drug3 %>% group_by(person_id) %>% distinct(.start, .end, .keep_all=T)


#data_drug <- data_drug[!(data_drug$.end-data_drug$.start== 0),]
#str(data_drug)
#df$ampm <- ifelse(as.numeric(format(data_drug$.start-data_drug$.end, '%d')) > 2, '1', '2')
data_1 <- data_drug3 %>% group_by(person_id, drug_concept_id, concept_name,group) %>% filter(.start==min(.start)) %>% select(person_id, drug_concept_id, concept_name, .start, group)
data_1 <- unique(data_1)
data_2 <- data_drug3 %>% group_by(person_id, drug_concept_id, concept_name,group) %>% filter(.end==max(.end)) %>% select(person_id, drug_concept_id, concept_name, .end, group)
data_2 <- unique(data_2)
data_final <- cbind(data_1, data_2)
data_final <- data_final[,-c(5:8,10)]
data_final$interval <- data_final$.end-data_final$.start+1
write_csv(data_final, ".csv")
          