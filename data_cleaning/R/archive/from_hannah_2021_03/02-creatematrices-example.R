library(readr)
library(dplyr)
library(tidyr)
library(reshape2) ## needed in the make.matrices function, should load after dplyr bc of conflicts, i think

code_dir <- "~/../Desktop/work/Psychometrics Data Cleaning Example"
source(paste0(code_dir,"/data_cleaning_functions.R"))
source(paste0(code_dir,"/clean.item.data.R"))

setwd("~/../Desktop/work/ad_hoc/BAR/2021 Diag Pilot Test")

analysis_name <- "Bar_Pilot"
data_path <- "Data"
results_path <- "Results"

## this also uses the test.map, so i often put it in a separate file so i can source it again
test.map <- readRDS(file = 'test.map.rds')

start_time <- Sys.time()
v <- TRUE
cleaning_output <- paste0(" Matrix checks for ", analysis_name )

## reading in the cleaned data
all_resp <- read_csv(file.path(results_path,
                               "Cleaned",
                               paste0(analysis_name," cleaned responses.csv")
                               ),
                         col_types = cols(student_id = col_character())
                     )

all_resp <- all_resp %>%
  arrange(template_name, item_section_position)


## initialize variables --------------------------------------------------------
## see below for why anything needs to be "fixed"
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



## "fix" scores for repeat Qs ----------------------------------------------------
## multiply all the score stuff by the inverse of whether it was omitted because it was repeated to zero out for the item pools
## this only matters because of the way the matrices were created - need to sum the values, and if an item was seen by a single user more than once,
## it will have multiple scores associated, and add them together, which is bad
replace.unattempted <- function(df, colname) {
  df[, paste0(colname,"_fixed")] <- replace(df[, colname],
                                            seq_along(df[, colname]),
                                            df[, colname] * !df$repeatOmitted)
  return(df)
}


for (z in seq_along(vars_to_fix)) {
  all_resp <- replace.unattempted(all_resp,vars_to_fix[z])
}


cleaning_output <- make.matrices(all_resp,
                                 vars_for_matrices = vars_for_matrix,
                                 matrix_names = matrix_names,
                                 destination_file_path = file.path(results_path),
                                 destination_file_name_prefix = paste0(analysis_name,"_User_level_"),
                                 cleaning_output,
                                 omit_code = ".",
                                 not_seen_code = "-99",
                                 use_display_order = TRUE)


## again just for scores, treating zero timed items as not seen and omits as incorrect, this is something Chen had wanted a few times in addition to what's in the DRCR
cleaning_output <- make.matrices(all_resp,
                                  vars_for_matrices = c("scored_response"),
                                  matrix_names = c("Item_Scores_Omit_Incorrect"),
                                  destination_file_path = file.path(results_path),
                                  destination_file_name_prefix = paste0(analysis_name,"_User_level_"),
                                  cleaning_output,
                                  omit_code = "0",
                                  not_seen_code = ".",
                                  use_display_order = TRUE,
                                  zero_sec_as_not_reached = TRUE)

stop_time <- Sys.time()
cleaning_output <- print.if.verbose(paste0("Ended at ", Sys.time(), ", Minutes elapsed: ", difftime(stop_time, start_time, units = "mins")), v = v, cleaning_output)

fileConn<-file(file.path(results_path, paste0(analysis_name, " Matrix creation information ", Sys.Date(),".txt")))
writeLines(cleaning_output, fileConn)
close(fileConn)