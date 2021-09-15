# -------------------------------------------------------------------------
# Set-up ------------------------------------------------------------------
# -------------------------------------------------------------------------

# Load initial packages
library(dplyr)
library(tidyr)
library(reshape2)
library(readr)
library(devtools)
#devtools::load_all() #?


# Set path to code
code_dir <- "~/../Desktop/work/ad_hoc/BAR/LAP-551/code"
source(paste0(code_dir,"/data_cleaning_functions.R"))
source(paste0(code_dir,"/clean.item.data.R"))


# -------------------------------------------------------------------------
# File paths --------------------------------------------------------------
# -------------------------------------------------------------------------

# Set working directory
main_dir <- "~/../Desktop/work/ad_hoc/BAR/LAP-551"
setwd(main_dir)

# Name directories
exam <- "16968"
analysis_name <- "bar_gbr"
data_path <- "data_pull"
results_path <- "results"


# Load data
ci_df <- read.csv(file.path(data_path, "content_info_all_piv.csv"), header = TRUE) 

# Select only items that a part of the current template
ci_df <- ci_df %>% filter(template_id == exam)

test.map <- readRDS(file = file.path(results_path,exam, paste0('test.map_',exam,'.rds')))

test_resp <- read_csv(file.path(results_path, exam, paste0(exam, "_cleaned_responses.csv")))

# -------------------------------------------------------------------------
# Create activity level info ----------------------------------------------
# -------------------------------------------------------------------------

activity_level_info <- test_resp %>%
  select(student_id, activity_id, timestamp_created, timestamp_completed, template_name,
           template_num_attempted, template_raw_correct, template_pTotal, template_pPlus) %>%
  distinct()

write_csv(activity_level_info, file.path(results_path, exam, paste0(exam,"_Activity_Level_Info.csv")))


# -------------------------------------------------------------------------
# Create User level info --------------------------------------------------
# -------------------------------------------------------------------------

test_resp_squished <- test_resp %>%
  group_by(student_id) %>%
  summarise(num_seq_taken = length(unique(template_name)),
            test_total = sum(test.map$num_ques),
            test_seen = n(),
            test_att = sum(attempted),
            test_correct = sum(scored_response),
            first_test_date = min(timestamp_created),
            last_test_date = max(timestamp_created)
            ) %>%
  mutate(test_ptotal = test_correct/test_seen,
         test_pplus = test_correct/test_att)

write_csv(test_resp_squished, file.path(results_path, exam, paste0(exam,"_User_Level_Info.csv")))

# -------------------------------------------------------------------------
# Create Content Item info ------------------------------------------------
# -------------------------------------------------------------------------

cor_ans <- get.item.cor.ans(test_resp)


cidf_summary <- test_resp %>%
  group_by(content_item_id, content_item_name, template_name, template_id, item_section_position) %>%
  summarise(count_att = sum(attempted),
            count_seen = length(content_item_id),
            num_correct = sum(scored_response),
            first_date = min(timestamp_created, na.rm = TRUE),
            last_date = max(timestamp_created, na.rm = TRUE)
  ) %>%
  arrange(template_name, item_section_position)

cidf_summary <- merge(cidf_summary, cor_ans, by = c("content_item_id"), all = TRUE) %>%
  arrange(content_item_id)

cidf_summary <- cidf_summary %>% filter(!is.na(count_att))

write_csv(cidf_summary,file.path(results_path, exam, paste0(exam,"_Content_Item_Info.csv")))

