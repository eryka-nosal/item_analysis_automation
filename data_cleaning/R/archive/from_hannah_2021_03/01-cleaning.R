# This file is the template cleaning file that I received and then modified from Hannah

## Load initial packages and set path ------------------------------------------

library(readr)
library(dplyr)
library(tidyr)

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


## in lieu of changing all the variable names in my code
resp_df <- resp_df %>%
  dplyr::rename(	#the format of dplyr::rename is new_name = old_name
    student_id = student_id,
    sequence_id = activity_id,
    item_position = item_position,
    section_title = section_title,
    item_section_position = item_section_position,
    content_item_id = content_item_id,
    seconds_used = seconds_used,
    is_scored = is_scored,
    scored_response = scored_response,
    raw_response = raw_response,
    response_status = item_status
  ) %>%
  mutate(section_name = section_title,
         correct_answer = NA)

## remove tutorial items + breaks
resp_df <- resp_df %>% filter(!is.na(item_section_position))

## sequenceInfo and format -------------------------------------------------
seq_df <- seq_df %>%
  dplyr::rename(
    student_id = student_id,
    sequence_id = activity_id,
    sequence_title = activity_title,
    sequence_status = status,
    template_id = template_id,
    timestamp_created = timestamp_created
  )

## used to have to combine two primary key columns from jasper, now just adding a column so my code doesn't break
seq_df$sequence_id_hist <- seq_df$sequence_id


# Merged response and seq
merged_df <- merge(resp_df, seq_df, by = c("student_id", "sequence_id","source_system"))

## take a look at the data ----------------------------------------------------

resp_df_summary <- merged_df %>%
  filter(sequence_status == 4) %>%
  group_by(template_id, template_name, student_id) %>% #section_title
  summarise(num_responses = n()) %>%
  ungroup() %>%
  group_by(template_id, template_name) %>% #section_title
  summarise(max_resp = max(num_responses),
            min_resp = min(num_responses),
            median_resp = median(num_responses),
            num_users = n())

## make maps ------------------------------------------------------------------
## this sample file is for an analysis over many quizzes, one row per sequence template/test or quiz form
#test.map <- read_csv("~/Kaplan/Psychometrics Data Cleaning Example/MCAT 7-Series EOC Quiz Sequences.csv")

## if the data is just for a qbank question pool, or for a single test form you only need one record. 
## below are the expected column names and some sample values, order is irrelevant
test.map <- resp_df_summary[,c("template_id","template_name")]
test.map$num_ques <- max(resp_df_summary$max_resp)
test.map$response_threshold = c(.75)
test.map$minutes_allowed = c(1440) #24 hours
test.map$score_names = c("scores") ## i don't think this gets used anymore?
#test.map$template_name = c("MCQ Skills") ## really sequence_name
test.map$test = c("BAR")
test.map$strings_as_factors = FALSE

saveRDS(test.map, file = 'test.map.rds')


## tracking initialization ------------------------------------------
start_time <- Sys.time()
cleaning_output <- paste0("Initial cleaning results ", analysis_name, Sys.Date())

v <- TRUE ## v stands for verbose, means i want it to output information to the console
num_seq_current <- length(unique(merged_df$sequence_id))
num_users_current <- length(unique(merged_df$student_id))
num_responses_current <- nrow(merged_df)
num_items_current <- length(unique(merged_df$content_item_id))
cleaning_output <- print.if.verbose(paste0("Total responses at start: ", num_responses_current), v, cleaning_output)
cleaning_output <- print.if.verbose(paste0("Total sequences at start: ", num_seq_current), v, cleaning_output)
cleaning_output <- print.if.verbose(paste0("Total users at start:     ", num_users_current), v, cleaning_output)
cleaning_output <- print.if.verbose(paste0("Unique content items at start: ", num_items_current), v, cleaning_output)

## connect to db ------------------------------------------------------
library(RPostgreSQL)
pg <- dbDriver("PostgreSQL")
redshift <- dbConnect(pg,
                      user="",
                      password="",
                      host="redshift-apps-clusterredshift-19qcp828fizxm.ctebqc6bt0fq.us-east-1.redshift.amazonaws.com",
                      port=5439,
                      dbname="redshiftapps")
## remove repeat students ------------------------------------------------------

repeaters <- dbGetQuery(redshift,"select ph.id kbs_enrollment_id
                        from kbs_billing.purchase_history ph
                        join kbs_catalog.product prod on ph.product_id = prod.id
                        where ph.initial_delta_k_txn_code in ('404','405','406') and ph.created_on >= '2019-01-01'
                        ")
merged_df <- merged_df %>% filter(!(kbs_enrollment_id %in% repeaters$kbs_enrollment_id))
num_seq_new <- length(unique(merged_df$sequence_id_hist))
num_users_new <- length(unique(merged_df$student_id))
num_responses_new <- nrow(merged_df)
num_items_new <- length(unique(merged_df$content_item_id))
cleaning_output <- print.if.verbose(paste0("responses removed from repeaters: ", num_responses_current - num_responses_new), v, cleaning_output)
cleaning_output <- print.if.verbose(paste0("Sequences removed: ", num_seq_current - num_seq_new), v, cleaning_output)
cleaning_output <- print.if.verbose(paste0("Users removed: ", num_users_current - num_users_new), v, cleaning_output)
cleaning_output <- print.if.verbose(paste0("Unique content items removed: ", num_items_current - num_items_new), v, cleaning_output)
num_seq_current <- num_seq_new
num_users_current <- num_users_new
num_responses_current <- num_responses_new
num_items_current <- num_items_new


## disconnect from db -------------------

dbDisconnect(redshift)
dbUnloadDriver(pg)


## remove 0 KBS EIDs ------------------------------------------------------
merged_df <- merged_df %>% filter(kbs_enrollment_id != 0)
num_seq_new <- length(unique(merged_df$sequence_id_hist))
num_users_new <- length(unique(merged_df$student_id))
num_responses_new <- nrow(merged_df)
num_items_new <- length(unique(merged_df$content_item_id))
cleaning_output <- print.if.verbose(paste0("responses removed from 0 KBS EID: ", num_responses_current - num_responses_new), v, cleaning_output)
cleaning_output <- print.if.verbose(paste0("Sequences removed: ", num_seq_current - num_seq_new), v, cleaning_output)
cleaning_output <- print.if.verbose(paste0("Users removed: ", num_users_current - num_users_new), v, cleaning_output)
cleaning_output <- print.if.verbose(paste0("Unique content items removed: ", num_items_current - num_items_new), v, cleaning_output)
num_seq_current <- num_seq_new
num_users_current <- num_users_new
num_responses_current <- num_responses_new
num_items_current <- num_items_new

## remove anyone who had a deleted sequence ---------------------------
users_w_deletes <- merged_df %>%
  mutate(deleted_name = grepl("[[:digit:]]_d",template_name)) %>%
  filter(sequence_status == "reset" | deleted_name) %>%
  select(student_id, kbs_enrollment_id, sequence_status, deleted_name, template_name, template_id)
## this removes users entirely
# merged_df <- merged_df %>% filter(!(student_id %in% users_w_deletes$student_id ))
## this removes only the sequences that share a template
merged_df <- merged_df %>% 
  anti_join(users_w_deletes %>% select(student_id, kbs_enrollment_id, template_id)) ## i didn't remove the other columns before so that the users_w_deletes object remains with additional info if we need to inspect it

num_seq_new <- length(unique(merged_df$sequence_id_hist))
num_users_new <- length(unique(merged_df$student_id))
num_responses_new <- nrow(merged_df)
num_items_new <- length(unique(merged_df$content_item_id))
cleaning_output <- print.if.verbose(paste0("responses removed for users w deleted seq: ", num_responses_current - num_responses_new), v, cleaning_output)
cleaning_output <- print.if.verbose(paste0("Sequences removed: ", num_seq_current - num_seq_new), v, cleaning_output)
cleaning_output <- print.if.verbose(paste0("Users removed: ", num_users_current - num_users_new), v, cleaning_output)
cleaning_output <- print.if.verbose(paste0("Unique content items removed: ", num_items_current - num_items_new), v, cleaning_output)
num_seq_current <- num_seq_new
num_users_current <- num_users_new
num_responses_current <- num_responses_new
num_items_current <- num_items_new

## clean and combine contentItemInfo -----------------------------------------------------

## the category names vary, you'll have to inspect the file, and you might want to keep more columns
ci_df <- ci_df %>%
	rename(
		content_item_id = question_number,
		template_id = product_name,
		section_title = exam_section_name
	) 

merged_df <- merge(merged_df, ci_df)


## check item correct answers in case cutoff table is needed --------------------------
cor_ans <- get.item.cor.ans(merged_df, no.correct.answer = TRUE)



## if the above returns an error and info to the console, there are answer key changes, and that breaks all my stuff. 
## this area is just some examples of how i've had to filter garbage out in the past
## 
## remove sequence with NA responses marked correct
# merged_df <- merged_df %>%
#   filter(!(sequence_id %in% c(40036550)))

# old_seq_to_remove <- merged_df %>%
#   filter(content_item_id == 279082 & timestamp_created <= "2020-04-06")
# merged_df <- merged_df %>%
#   filter(!(sequence_id %in% old_seq_to_remove$sequence_id))

## and sometimes I create a cutoff table that the function below consumes
# cutoff_items <- data.frame(content_item_name = "n033496",
#                            cutoff_date = "2020-08-11",
#                            stringsAsFactors = FALSE)

## clean data w function ------------------------------------------------------------------
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
                                     remove.no.response.scored = FALSE,
                                     remove.over.time.sequences = TRUE,
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
                                     # seqdf = seq_df,
                                     interaction.type.list = c(1,10,11,23,24,25,19),
                                     v = TRUE,
                                     all.or.nothing = TRUE,
                                     section.calc = FALSE,
                                     section.separated = FALSE)
cleaning_output <- paste0(cleaning_output,"\r\n",cleaned.resp_df.list[[1]])
for (q in 2:length(cleaned.resp_df.list)) {
  if(dim(cleaned.resp_df.list[[q]]) > 0) {
    write_csv(cleaned.resp_df.list[[q]],file.path(results_path,"Cleaned",
                                               paste0(names(cleaned.resp_df.list)[q], " cleaned responses.csv")))
  }
}


## combine and write cleaning information --------------------------------------

stop_time <- Sys.time()
cleaning_output <- print.if.verbose(paste0("Minutes elapsed: ", difftime(stop_time, start_time, units = "mins")), v, cleaning_output)
fileConn<-file(file.path(results_path, paste0(analysis_name," cleaning information ",".txt")))
writeLines(cleaning_output, fileConn)
close(fileConn)




