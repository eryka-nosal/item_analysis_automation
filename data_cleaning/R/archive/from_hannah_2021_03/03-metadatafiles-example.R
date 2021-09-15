library(dplyr)
library(tidyr)
library(reshape2)
library(readr)
library(devtools)
#devtools::load_all() #?
## read in cleaned data files --------------------------------------------------

code_dir <- "~/../Desktop/work/Psychometrics Data Cleaning Example"
source(paste0(code_dir,"/data_cleaning_functions.R"))
source(paste0(code_dir,"/clean.item.data.R"))

setwd("~/../Desktop/work/ad_hoc/BAR/2021 Diag Pilot Test")

analysis_name <- "Bar_Pilot"
data_path <- "Data"
results_path <- "Results"


## load data
resp_df <- read_tsv(file.path(data_path, "response_data.tsv"))
seq_df <- read_tsv(file.path(data_path,"sequence_info.tsv"))
ci_df <- read_tsv(file.path(data_path, "content_item_info.tsv"))


test_resp <- read_csv(file.path(results_path, "Cleaned", "/Bar_Pilot cleaned responses.csv"))


### Create Sequence_Level_Info
sequence_level_info <- test_resp %>%
  select(student_id, sequence_id, timestamp_created, timestamp_completed, template_name,
           template_num_attempted, template_raw_correct, template_pTotal, template_pPlus) %>%
  distinct()

write_csv(sequence_level_info, file.path(results_path, paste0(analysis_name,"_Sequence_Level_Info.csv")))


### Create User_Level_Info

test_resp_squished <- test_resp %>%
  group_by(student_id) %>%
  summarise(num_seq_taken = length(unique(template_name)),
            test_total = sum(test.map$numQues),
            test_seen = n(),
            test_att = sum(attempted),
            test_correct = sum(scored_response),
            first_test_date = min(timestamp_created),
            last_test_date = max(timestamp_created)
            ) %>%
  mutate(test_ptotal = test_correct/test_seen,
         test_pplus = test_correct/test_att)

write_csv(test_resp_squished, file.path(results_path, paste0(analysis_name,"_User_Level_Info.csv")))

### Create Content_Item_Info.csv

cor_ans <- get.item.cor.ans(test_resp, no.correct.answer = TRUE,use.content.item.name = FALSE)

cidf_summary <- test_resp %>%
  group_by(content_item_id, content_item_name, template_name, template_id, item_section_position) %>%
  summarise(count_att = sum(attempted),
            count_seen = length(content_item_id),
            num_correct = sum(scored_response),
            first_date = min(timestamp_created, na.rm = TRUE),
            last_date = max(timestamp_created, na.rm = TRUE)
  ) %>%
  arrange(template_name, item_section_position)

cidf_summary <- merge(cidf_summary, ci_df, all.x = TRUE) %>%
  merge(., cor_ans, all = TRUE) %>%
  arrange(content_item_name)

cidf_summary <- cidf_summary %>% filter(!is.na(count_att))

write_csv(cidf_summary,file.path(results_path, paste0(analysis_name,"_Content_Item_Info.csv")))

