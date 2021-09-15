# -------------------------------------------------------------------------
# Set-up ------------------------------------------------------------------
# -------------------------------------------------------------------------

# Load initial packages
library(readr)
library(dplyr)
library(tidyr)
library(reshape2)

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


## Load data
test.map <- readRDS(file = file.path(results_path,exam, paste0('test.map_',exam,'.rds')))


## reading in the cleaned data
all_resp <- read_csv(file.path(results_path, exam, paste0(exam,"_cleaned_responses.csv")), col_types = cols(student_id = col_character()))

all_resp <- all_resp %>%
  arrange(template_name, item_section_position)

# -------------------------------------------------------------------------
# Tracking initialization -------------------------------------------------
# -------------------------------------------------------------------------

start_time <- Sys.time()
v <- TRUE
cleaning_output <- paste0(" Matrix checks for ", analysis_name )

## initialize variables
vars_to_fix <- c("milliseconds_used"
                 # "template_raw_correct",
                 # "overall_raw_correct",
                 # "template_pPlus",
                 # "overall_pPlus",
                 # "template_pTotal",
                 # "overall_pTotal",
                 # "sequence_q_order",
                 # "overall_order",
                 # "section_scaled_score",
                 # "total",
                 # "sequence_order"
)
vars_fixed <- paste0(vars_to_fix,"_fixed")
matrix_names <- c("Item_Scores",
                  "Responses",
                  "Milliseconds_per_Item"
                  # "Raw_Correct_by_Sequence",
                  # "Raw_Correct_Overall",
                  # "Perc_Correct_pPlus",
                  # "Perc_Correct_pPlus_Overall",
                  # "Perc_Correct_pTotal",
                  # "Perc_Correct_pTotal_Overall",
                  # "Item_Order_Sequence",
                  # "Item_Order_Overall",
                  # "Criterion_Scaled_score_SS",
                  # "Criterion_Score",
                  # "Item_Membership"
)


vars_for_matrix <- c("scored_response", "raw_response", vars_fixed)


# -------------------------------------------------------------------------
# "Fix" scores for repeat Qs ----------------------------------------------
# -------------------------------------------------------------------------

replace.unattempted <- function(df, colname) {
  df[, paste0(colname,"_fixed")] <- replace(df[, colname],
                                            seq_along(df[, colname]),
                                            df[, colname] * !df$repeatOmitted)
  return(df)
}


for (z in seq_along(vars_to_fix)) {
  all_resp <- replace.unattempted(all_resp,vars_to_fix[z])
}


# -------------------------------------------------------------------------
# Make matrices -----------------------------------------------------------
# -------------------------------------------------------------------------


cleaning_output <- make.matrices(all_resp,
                                 vars_for_matrices = vars_for_matrix,
                                 matrix_names = matrix_names,
                                 destination_file_path = file.path(results_path,exam),
                                 destination_file_name_prefix = paste0(exam,"_User_level_"),
                                 cleaning_output,
                                 omit_code = ".",
                                 not_seen_code = "-99",
                                 use_display_order = TRUE)

# -------------------------------------------------------------------------
# Stop Process, Write-out -------------------------------------------------
# -------------------------------------------------------------------------

stop_time <- Sys.time()
cleaning_output <- print.if.verbose(paste0("Ended at ", Sys.time(), ", Minutes elapsed: ", difftime(stop_time, start_time, units = "mins")), v = v, cleaning_output)

fileConn<-file(file.path(results_path, exam, paste0(exam, "_matrix_creation_info_", Sys.Date(),".txt")))
writeLines(cleaning_output, fileConn)
close(fileConn)