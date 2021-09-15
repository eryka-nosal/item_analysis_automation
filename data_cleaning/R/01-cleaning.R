# -------------------------------------------------------------------------
# Set-up ------------------------------------------------------------------
# -------------------------------------------------------------------------

# Load initial packages
library(readr)
library(dplyr)
library(tidyr)

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
exam <- "46"
analysis_name <- "bar_gbr"
data_path <- "data_pull"
results_path <- "results"

# Create directories
if (!dir.exists(file.path(main_dir, results_path))) {
  dir.create(file.path(main_dir, results_path))
}
if (!dir.exists(file.path(main_dir, results_path,exam))) {
  dir.create(file.path(main_dir, results_path,exam))
}

# Manually drag activity, response, content files into the data_path file

# Load data
resp_df <- read.csv(file.path(data_path, paste0("response_info_", exam, ".csv")), na.strings='NULL', header = TRUE)
act_df <- read.csv(file.path(data_path, paste0("activity_info_", exam, ".csv")), na.strings='NULL',  header = TRUE)  
ci_df <- read.csv(file.path(data_path, "content_info_all_piv.csv"),na.strings='NULL', header = TRUE) 

# Select only items that a part of the current template
ci_df <- ci_df %>% filter(template_id == exam)

# -------------------------------------------------------------------------
# Response df: Define columns ---------------------------------------------
# -------------------------------------------------------------------------

resp_df <- resp_df %>%
  dplyr::rename(
    source_system = ï..source_system,
    student_id = student_id,
    activity_id = activity_id,
    item_position = item_position,
    section_title = section_title,
    item_section_position = item_section_position,
    content_item_id = content_item_id,
    content_item_name = content_item_name,
    milliseconds_used = milliseconds_used,
    is_scored = is_scored,
    scored_response = scored_response,
    raw_response = max_raw_answer,
    response_status = item_status
    ) %>%
  mutate(section_name = section_title,
         rawest_response = raw_response,
         #milliseconds_used = seconds_used * 1000,
         correct_answer = NA,
         template_name = template_id
   )


## remove tutorial items + breaks
resp_df <- resp_df %>% filter(!is.na(item_section_position))


# -------------------------------------------------------------------------
# activityInfo and format -------------------------------------------------
# -------------------------------------------------------------------------
act_df <- act_df %>%
  dplyr::rename(
    source_system = ï..source_system,
    student_id = student_id,
    kbs_enrollment_id = kbs_enrollment_id,
    activity_id = activity_id,
    activity_title = activity_name,
    activity_status = status,
    template_id = template_id,
    template_name = template_name,
    timestamp_created = date_created,
    timestamp_completed = date_completed
  ) %>%
  mutate(
    activity_id_hist = activity_id,
    template_name = template_id
  )


# -------------------------------------------------------------------------
# Merge resp and seq dfs --------------------------------------------------
# -------------------------------------------------------------------------

merged_df <- merge(resp_df, act_df, by = c("student_id", "activity_id","history_db_id","template_id","template_name","source_system"))

# Create a summary
resp_df_summary <- merged_df %>%
  filter(activity_status == 'Complete') %>%
  group_by(template_id, template_name, student_id) %>% 
  summarise(num_responses = n()) %>%
  ungroup() %>%
  group_by(template_id, template_name) %>%
  summarise(max_resp = max(num_responses),
            min_resp = min(num_responses),
            median_resp = median(num_responses),
            num_users = n())

# -------------------------------------------------------------------------
# Make test maps ----------------------------------------------------------
# -------------------------------------------------------------------------

test.map <- resp_df_summary[,c("template_id","template_name")]
test.map$num_ques <- length(unique(ci_df$content_item_id)) #max(resp_df_summary$max_resp)
test.map$response_threshold = c(.75)
test.map$minutes_allowed = c(180) #1440 24 hours, 180min (3hours) for 100q, 270 min (4.5 hours) for 150q, 360 min (6 hours) for 200q,
#test.map$template_name = exam
test.map$test = exam
test.map$strings_as_factors = FALSE

saveRDS(test.map, file = file.path(results_path, exam, paste0('test.map_',exam,'.rds')))


# -------------------------------------------------------------------------
# Tracking initialization -------------------------------------------------
# -------------------------------------------------------------------------

start_time <- Sys.time()

cleaning_output <- paste0("Initial cleaning results ", analysis_name, Sys.Date())
v <- TRUE ## v stands for verbose, means i want it to output information to the console

# Initial counts
num_seq_current <- length(unique(merged_df$activity_id))
num_users_current <- length(unique(merged_df$student_id))
num_responses_current <- nrow(merged_df)
num_items_current <- length(unique(merged_df$content_item_id))

# Print cleaning summary
cleaning_output <- print.cleaning.output(cleaning_output, v, "Initial Count", num_responses_current, num_seq_current, num_users_current, num_items_current)


# -------------------------------------------------------------------------
# Connect to DB for cleaning ----------------------------------------------
# -------------------------------------------------------------------------

# set-up
library(RPostgreSQL)
pg <- dbDriver("PostgreSQL")

# connection info
redshift <- dbConnect(pg,
                      user="",
                      password="",
                      host="redshift-apps-clusterredshift-19qcp828fizxm.ctebqc6bt0fq.us-east-1.redshift.amazonaws.com",
                      port=5439,
                      dbname="redshiftapps")

# query for repeaters
repeaters <- dbGetQuery(redshift,"select ph.id kbs_enrollment_id
                        from kbs_billing.purchase_history ph
                        join kbs_catalog.product prod on ph.product_id = prod.id
                        where ph.initial_delta_k_txn_code in ('404','405','406') and ph.created_on >= '2018-01-01'
                        ")

# query for free trials/online companions
freetrialers <- dbGetQuery(redshift,"select distinct ph.id kbsenrollmentid 
                                  from kbs_billing.purchase_history ph
                                  join bi_reporting.vw_product_detail prd on ph.product_id = prd.product_id
                                  where prd.product_code in ('EDGE1LO', 'MBE2LA13', 'BRMPREO13', 'BRMPREOB3')
                                  and ph.created_on >= '2018-01-01'
                                  ")


## disconnect from db
dbDisconnect(redshift)
dbUnloadDriver(pg)

# filter for repeaters
merged_df <- merged_df %>% filter(!(kbs_enrollment_id %in% repeaters$kbs_enrollment_id))

# Get new counts
num_seq_new <- length(unique(merged_df$activity_id_hist))
num_users_new <- length(unique(merged_df$student_id))
num_responses_new <- nrow(merged_df)
num_items_new <- length(unique(merged_df$content_item_id))

#Print removal summary
cleaning_output <- print.removal.output(cleaning_output, v, "Remove repeaters", 
                     num_responses_current - num_responses_new, 
                     num_seq_current - num_seq_new, 
                     num_users_current - num_users_new, 
                     num_items_current - num_items_new)

# Update current counts
num_seq_current <- num_seq_new
num_users_current <- num_users_new
num_responses_current <- num_responses_new
num_items_current <- num_items_new


# Filter for free trials/online companions
merged_df <- merged_df %>% filter(!(kbs_enrollment_id %in% freetrialers$kbs_enrollment_id))

# Get new counts
num_seq_new <- length(unique(merged_df$activity_id_hist))
num_users_new <- length(unique(merged_df$student_id))
num_responses_new <- nrow(merged_df)
num_items_new <- length(unique(merged_df$content_item_id))

#Print removal summary
cleaning_output <- print.removal.output(cleaning_output, v, "Remove free trials/online companions", 
                                        num_responses_current - num_responses_new, 
                                        num_seq_current - num_seq_new, 
                                        num_users_current - num_users_new, 
                                        num_items_current - num_items_new)

# Update current counts
num_seq_current <- num_seq_new
num_users_current <- num_users_new
num_responses_current <- num_responses_new
num_items_current <- num_items_new


# remove 0 KBS EIDs
merged_df <- merged_df %>% filter(kbs_enrollment_id != 0)

# Get new counts
num_seq_new <- length(unique(merged_df$activity_id_hist))
num_users_new <- length(unique(merged_df$student_id))
num_responses_new <- nrow(merged_df)
num_items_new <- length(unique(merged_df$content_item_id))

#Print removal summary
cleaning_output <- print.removal.output(cleaning_output, v, "Remove users with 0 KBS EIDs", 
                     num_responses_current - num_responses_new, 
                     num_seq_current - num_seq_new, 
                     num_users_current - num_users_new, 
                     num_items_current - num_items_new)

# Update current counts
num_users_current <- num_users_new
num_responses_current <- num_responses_new
num_items_current <- num_items_new

# remove anyone who had a deleted activity ---------------------------
users_w_deletes <- merged_df %>%
  mutate(deleted_name = grepl("[[:digit:]]_d",template_name)) %>%
  filter(activity_status == "reset" | deleted_name) %>%
  select(student_id, kbs_enrollment_id, activity_status, deleted_name, template_name, template_id)
## this removes users entirely
# merged_df <- merged_df %>% filter(!(student_id %in% users_w_deletes$student_id ))
## this removes only the activities that share a template
merged_df <- merged_df %>% 
  anti_join(users_w_deletes %>% select(student_id, kbs_enrollment_id, template_id)) ## i didn't remove the other columns before so that the users_w_deletes object remains with additional info if we need to inspect it

# Get new counts
num_seq_new <- length(unique(merged_df$activity_id_hist))
num_users_new <- length(unique(merged_df$student_id))
num_responses_new <- nrow(merged_df)
num_items_new <- length(unique(merged_df$content_item_id))

#Print removal summary
cleaning_output <- print.removal.output(cleaning_output, v, "Remove deleted activities", 
                     num_responses_current - num_responses_new, 
                     num_seq_current - num_seq_new, 
                     num_users_current - num_users_new, 
                     num_items_current - num_items_new)

# Update current counts
num_seq_current <- num_seq_new
num_users_current <- num_users_new
num_responses_current <- num_responses_new
num_items_current <- num_items_new


# -------------------------------------------------------------------------
# Clean and combine ContentItemInfo ---------------------------------------
# -------------------------------------------------------------------------

ci_df <- ci_df %>%
  select(template_id, content_item_id, content_item_name, answer_index) %>%
  rename(correct_answer = answer_index)

merged_df <- select(merged_df, -correct_answer)

merged_df <- merge(merged_df, ci_df, by = c("template_id","content_item_id","content_item_name"))

cor_ans <- get.item.cor.ans(merged_df)




# -------------------------------------------------------------------------
# Main Clean Data Function ------------------------------------------------
# -------------------------------------------------------------------------
cleaned.resp_df.list <- clean.item.data(data_path = data_path,
                                     results_path = results_path,
                                     df = merged_df,
                                     analysis.name = analysis_name,
                                     test.map = test.map,
                                     #section.map = section.map,
                                     qbank = FALSE,
                                     detect.section.totals = FALSE,
                                     remove.unscored = FALSE,
                                     remove.incomplete = TRUE,
                                     remove.tutor = TRUE,
                                     remove.no.kbsEID = TRUE,
                                     repeat.treatment = "omit",
                                     remove.no.response.scored = TRUE,#EN, 5/26/21 - I set this to true. MCAT FL had a 'correct' response of NA
                                     remove.over.time.activities = TRUE,
                                     remove.repeat.test.administrations = TRUE,
                                     recode.answers = TRUE,
                                     #seqHist.to.exclude = seqHist.to.exclude,
                                     #total.minutes.threshold = 480,
                                     mSec.min.threshold = 5000,
                                     #mSec.max.threshold = 600000,
                                     # CI.old.keys = CI.old.keys,
                                     # CI.old.version.dates = cutoff_items,
                                     # field.test.items = field_test_items,
                                     precombined.files = TRUE,
                                     # cidf = ci_df,
                                     # seqdf = act_df,
                                     interaction.type.list = c(1,10,11,23,24,25,19),
                                     v = TRUE,
                                     all.or.nothing = TRUE,
                                     section.calc = FALSE,
                                     section.separated = FALSE)


cleaning_output <- paste0(cleaning_output,"\r\n",cleaned.resp_df.list[[1]])
write_csv(cleaned.resp_df.list[[2]],file.path(results_path,exam,paste0(exam, "_cleaned_responses.csv")))


# -------------------------------------------------------------------------
# Stop Process, Write-out -------------------------------------------------
# -------------------------------------------------------------------------

stop_time <- Sys.time()
cleaning_output <- print.if.verbose(paste0("Minutes elapsed: ", difftime(stop_time, start_time, units = "mins")), v, cleaning_output)

# Write-out all cleaning output to data
fileConn<-file(file.path(results_path, exam, paste0(exam,"_cleaning_info",".txt")))
writeLines(cleaning_output, fileConn)
close(fileConn)