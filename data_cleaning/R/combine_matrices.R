library(dplyr)
library(tidyr)
library(reshape2)
library(readr)

# -------------------------------------------------------------------------
# Read-in files -----------------------------------------------------------
# -------------------------------------------------------------------------


main_dir <- "~/../Desktop/work/ad_hoc/MCAT/LAP-545"
setwd(main_dir)

fl_exam <- "combined"
analysis_name <- "mcat_fl"
data_path <- "data_pull"
results_path <- "results"

#############
## AAMC PE ##
#############

aamc_path <- "~/../Desktop/work/ad_hoc/MCAT/LAP-538"

# User level info
AAMCPE1_users_df <- read.csv(file.path(aamc_path,"results/PE1/mcat_User_level_Info_PE1.csv"))[1]
AAMCPE2_users_df <- read.csv(file.path(aamc_path,"results/PE2/mcat_User_level_Info_PE2.csv"))[1]
AAMCPE3_users_df <- read.csv(file.path(aamc_path,"results/PE3/mcat_User_level_Info_PE3.csv"))[1]
AAMCPE4_users_df <- read.csv(file.path(aamc_path,"results/PE4/mcat_User_level_Info_PE4.csv"))[1]
ids <- read.csv(file.path(data_path,"MCAT_id_lookup.csv"))

# Item Scores
AAMCPE1_scores_df <- read.csv(file.path(aamc_path,"results/PE1/mcat_User_level_Item_Scores_PE1.csv"))
AAMCPE2_scores_df <- read.csv(file.path(aamc_path,"results/PE2/mcat_User_level_Item_Scores_PE2.csv"))
AAMCPE3_scores_df <- read.csv(file.path(aamc_path,"results/PE3/mcat_User_level_Item_Scores_PE3.csv"))
AAMCPE4_scores_df <- read.csv(file.path(aamc_path,"results/PE4/mcat_User_level_Item_Scores_PE4.csv"))

# Item Responses
AAMCPE1_responses_df <- read.csv(file.path(aamc_path,"results/PE1/mcat_User_level_Responses_PE1.csv"))
AAMCPE2_responses_df <- read.csv(file.path(aamc_path,"results/PE2/mcat_User_level_Responses_PE2.csv"))
AAMCPE3_responses_df <- read.csv(file.path(aamc_path,"results/PE3/mcat_User_level_Responses_PE3.csv"))
AAMCPE4_responses_df <- read.csv(file.path(aamc_path,"results/PE4/mcat_User_level_Responses_PE4.csv"))

# Milliseconds per Item
AAMCPE1_ms_df <- read.csv(file.path(aamc_path,"results/PE1/mcat_User_level_Milliseconds_per_Item_PE1.csv"))
AAMCPE2_ms_df <- read.csv(file.path(aamc_path,"results/PE2/mcat_User_level_Milliseconds_per_Item_PE2.csv"))
AAMCPE3_ms_df <- read.csv(file.path(aamc_path,"results/PE3/mcat_User_level_Milliseconds_per_Item_PE3.csv"))
AAMCPE4_ms_df <- read.csv(file.path(aamc_path,"results/PE4/mcat_User_level_Milliseconds_per_Item_PE4.csv"))

#############
## MCAT FL ##
#############

fl1_scores <- read.csv("results/fl1/fl1_User_level_Item_Scores.csv")
fl2_scores <- read.csv("results/fl2/fl2_User_level_Item_Scores.csv")
fl3_scores <- read.csv("results/fl3/fl3_User_level_Item_Scores.csv")
fl7_scores <- read.csv("results/fl7/fl7_User_level_Item_Scores.csv")

fl1_responses <- read.csv("results/fl1/fl1_User_level_Responses.csv")
fl2_responses <- read.csv("results/fl2/fl2_User_level_Responses.csv")
fl3_responses <- read.csv("results/fl3/fl3_User_level_Responses.csv")
fl7_responses <- read.csv("results/fl7/fl7_User_level_Responses.csv")

fl1_ms <- read.csv("results/fl1/fl1_User_level_Milliseconds_per_Item.csv")
fl2_ms <- read.csv("results/fl2/fl2_User_level_Milliseconds_per_Item.csv")
fl3_ms <- read.csv("results/fl3/fl3_User_level_Milliseconds_per_Item.csv")
fl7_ms <- read.csv("results/fl7/fl7_User_level_Milliseconds_per_Item.csv")


# -------------------------------------------------------------------------
# Functions ---------------------------------------------------------------
# -------------------------------------------------------------------------

#Renames the 'X' column based on whether the exams are from KNA (Full length exams) or from AAMC (Practice Exams)
## If KNA exams, then the student identifier is 'student_id'
## If AAMC exams, then the student identifier is 'kaplan_user_id'
rename_df <- function(df, exam_source) {
  if (exam_source == "KNA") {
    df <- df %>% 
      dplyr::rename(student_id = X)  
  } else if (exam_source == "AAMC") {
    df <- df %>% 
      dplyr::rename(kaplan_user_id = X)  
  }
}

merge_dfs <-function(df_list){
  df_merged <- Reduce(
    function(x, y, ...) merge(x, y, all = TRUE,),df_list)
  return(df_merged)
}


combine_dfs <- function(df1, df2, df3, df4, exam_source, prefix_list) {
  #rename columns
  df1 <- df1 %>% rename_with( ~ paste0(prefix_list[1], .x), -c('X'))
  df2 <- df2 %>% rename_with( ~ paste0(prefix_list[2], .x), -c('X'))
  df3 <- df3 %>% rename_with( ~ paste0(prefix_list[3], .x), -c('X'))
  df4 <- df4 %>% rename_with( ~ paste0(prefix_list[4], .x), -c('X'))
  
  
  # Rename id with student identifiers
  df1 <- rename_df(df1, exam_source)
  df2 <- rename_df(df2, exam_source)
  df3 <- rename_df(df3, exam_source)
  df4 <- rename_df(df4, exam_source)
  
  
  # # filter each df down to only those users in 'filter_users'
  # df1 <- merge(df1,filter_users)
  # df2 <- merge(df2,filter_users)
  # df3 <- merge(df3,filter_users)
  # df4 <- merge(df4,filter_users)
  
  df_list <- list(df1,df2,df3,df4)
  
  # # # Merge all MCAT FL exams together
  df_merged <- merge_dfs(df_list)

  return(df_merged)
  
}



# -------------------------------------------------------------------------
# Merge -------------------------------------------------------------------
# -------------------------------------------------------------------------

# Merge AAMC PE responses into one df
AAMC_PE_responses <- combine_dfs(AAMCPE1_responses_df, AAMCPE2_responses_df, AAMCPE3_responses_df, AAMCPE4_responses_df, "AAMC", list("PE1_","PE2_","PE3_","PE4_"))
AAMC_PE_responses <- merge(AAMC_PE_responses, ids, by = c('kaplan_user_id'))#, all.x= TRUE)


# Merge AAMC PE Scores into one df
AAMC_PE_scores <- combine_dfs(AAMCPE1_scores_df, AAMCPE2_scores_df, AAMCPE3_scores_df, AAMCPE4_scores_df, "AAMC", list("PE1_","PE2_","PE3_","PE4_"))
AAMC_PE_scores <- merge(AAMC_PE_scores, ids, by = c('kaplan_user_id'))#, all.x= TRUE)


# Merge AAMC PE ms into one df
AAMC_PE_ms <- combine_dfs(AAMCPE1_ms_df, AAMCPE2_ms_df, AAMCPE3_ms_df, AAMCPE4_ms_df, "AAMC", list("PE1_","PE2_","PE3_","PE4_"))
AAMC_PE_ms <- merge(AAMC_PE_ms, ids, by = c('kaplan_user_id'))#, all.x= TRUE)


# Merge MCAT FL responses into one df
FL_responses <- combine_dfs(fl1_responses, fl2_responses, fl3_responses, fl7_responses, "KNA", list("FL1_","FL2_","FL3_","FL7_"))

# Merge MCAT FL Scores into one df
FL_scores <- combine_dfs(fl1_scores, fl2_scores, fl3_scores, fl7_scores, "KNA", list("FL1_","FL2_","FL3_","FL7_"))

# Merge MCAT FL ms into one df
FL_ms <- combine_dfs(fl1_ms, fl2_ms, fl3_ms, fl7_ms, "KNA", list("FL1_","FL2_","FL3_","FL7_"))




# Combine MCAT FL & AAMC PE dfs - responses
filtered_responses <- merge(AAMC_PE_responses, FL_responses)

# Combine MCAT FL & AAMC PE dfs - scores
filtered_scores <- merge(AAMC_PE_scores, FL_scores)

# Combine MCAT FL & AAMC PE dfs - scores
filtered_ms <- merge(AAMC_PE_ms, FL_ms)


# -------------------------------------------------------------------------
# Write-out ---------------------------------------------------------------
# -------------------------------------------------------------------------
out_dir <- "all_items_MCAT-FL_and_AAMC-PE"

# Create directories
if (!dir.exists(file.path(main_dir, results_path, out_dir))) {
  dir.create(file.path(main_dir, results_path, out_dir))
}

write_csv(filtered_responses, file.path(main_dir, results_path, out_dir, "MCAT-FL_AAMC-PE_all_items_responses.csv"))
write_csv(filtered_scores, file.path(main_dir, results_path, out_dir, "MCAT-FL_AAMC-PE_all_items_scores.csv"))
write_csv(filtered_ms, file.path(main_dir, results_path, out_dir, "MCAT-FL_AAMC-PE_all_items_ms.csv"))