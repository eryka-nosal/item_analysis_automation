

#' Clean and combine item response data and test & content item level information
#'
#' @param data_path
#' @param results_path
#' @param df
#' @param analysis.name
#' @param section.map list with specific structure
#' @param detect.section.totals
#' @param scores.to.include
#' @param date.columns
#' @param date.formats
#' @param remove.unscored
#' @param remove.incomplete
#' @param remove.tutor
#' @param remove.no.kbsEID
#' @param repeat.treatment
#' @param recode.answers
#' @param seqHist.to.exclude
#' @param precombined.files
#' @param total.minutes.threshold
#' @param mSec.min.threshold
#' @param mSec.max.threshold
#' @param sec.min.threshold
#' @param sec.max.threshold
#' @param ci.cols.to.include
#' @param interaction.type.list
#' @param cidf
#' @param CI.old.keys
#' @param field.test.items
#'
#' @return list of data frames, one per "row" in section.map
#' @export
#'
#' @examples
clean.item.data <- function(data_path,
                            results_path = data_path,
                            df = NULL,
                            analysis.name,
                            test.map = NULL,
                            section.map = NULL,
                            qbank = FALSE,
                            detect.section.totals = FALSE,
                            scores.to.include = "overall",
                            date.columns = c("timestamp_created","timestamp_completed"),
                            date.formats = c("%B %d %Y %I:%M:%OS %p","%B %d %Y %I:%M:%OS %p"),
                            remove.unscored = FALSE,
                            remove.incomplete = TRUE,
                            remove.tutor = TRUE,
                            remove.no.kbsEID = TRUE,
                            repeat.treatment = "omit",
                            seqHist.to.exclude = NULL,
                            precombined.files = TRUE,
                            remove.no.response.scored = TRUE,
                            remove.over.time.sequences = TRUE,
                            remove.repeat.test.administrations = FALSE,
                            recode.answers = FALSE,
                            total.minutes.threshold = NULL,
                            mSec.min.threshold = NULL,
                            mSec.max.threshold = NULL,
                            sec.min.threshold = NULL,
                            sec.max.threshold = NULL,
                            min.items.per.seq = NULL,
                            timing.excl.map = NULL,
                            ci.cols.to.include = NULL,
                            interaction.type.list = 1,
                            cidf = NULL,
                            seqdf = NULL,
                            CI.old.keys = NULL,
                            CI.old.version.dates = NULL,
                            CI.remove.before.after = "before",
                            CI.old.version.list = NULL,
                            field.test.items = NULL,
                            v = TRUE,
                            all.or.nothing = FALSE,
                            section.calc = TRUE,
                            section.separated = FALSE) {
# inputs
#
# test name
# sections/categories and what column they are located in - this is a small data frame where the first column is the prettified section name, the second is the jasper section name, the third is how many items are expected in that section
# if section, total number administered as denominator for total responses expected (important for GMAT)
# which final calculated scores (+thetas for adaptive tests) are worth including in the cleaned data
# data location/path
# which columns are dates and numbers
# whether to remove/omit repeat questions
# timing exclusion # of mSec
# seq IDs to exclude
# list of FT items if any, or other category not in the source data
# threshold for % of questions answered to allow sequence into analysis
# whether to remove unscored items

  
  
  if (is.null(test.map) & is.null(section.map)) {
    stop("No section.map or test.map!")
  }
  cleaning_info <- paste0("Starting clean item data function at ", Sys.time())

  if (is.null(df)) {
    stop("No response df")
  }

  num_seq_current <- length(unique(df$sequence_id_hist))
  num_users_current <- length(unique(df$student_id))
  num_items_current <- length(unique(df$content_item_name))
  cleaning_info <- print.if.verbose(paste0("Total sequences at start: ", num_seq_current), v, cleaning_info)
  cleaning_info <- print.if.verbose(paste0("Total users at start: ", num_users_current), v, cleaning_info)
  cleaning_info <- print.if.verbose(paste0("Unique items at start: ", num_items_current), v, cleaning_info)

  ## SEQUENCE LEVEL EXCLUSIONS
  if(!is.null(section.map)) {
      resp_excl <- df %>% ## only sections in the section map
        dplyr::filter(sectionName %in% unlist(section.map$jasperSectionName))
      removed.record.count(resp_excl, thing.to.say = "Sequences without responses specified in the section map, removed: ")
  } else {
    resp_excl <- df
  }
  initial_columns <- names(resp_excl)

  if(remove.no.kbsEID == TRUE) {
    resp_excl <- resp_excl %>%
      filter(!is.na(kbs_enrollment_id))
    removed.record.count(resp_excl, thing.to.say = "Sequences with no KBS EID removed: ")
  }
  if(remove.incomplete == TRUE) {
    resp_excl <- resp_excl %>%
      dplyr::filter(sequence_status == 4 | sequence_status == "completed")
    removed.record.count(resp_excl, thing.to.say = "Non Complete sequences removed: ")
  }
  if(remove.tutor == TRUE) {
    resp_excl <- resp_excl %>%
      dplyr::filter(tutor_mode != "True" | is.na(tutor_mode)) ## accounting for possibility of empty tutor_mode field - empty would mean "False" which means FALSE
    removed.record.count(resp_excl, thing.to.say = "Tutor mode sequences removed: ")
  }
  if(!is.null(total.minutes.threshold)) {
    resp_excl <- resp_excl %>% ## exclude sequences that took longer than specified time to complete
      dplyr::mutate(total_time = as.numeric(difftime(timestamp_completed, timestamp_created, units = "mins")))
    # print(paste0("Number of sequences with more than ",total.minutes.threshold," minutes: ",dim(resp_excl %>% dplyr::filter(total_time > total.minutes.threshold) %>% select(sequence_id_hist) %>% distinct())[1]))
    resp_excl <- resp_excl %>%
      dplyr::filter(total_time <= total.minutes.threshold) ## defaulted to 1440 minutes (24 hours) in parameters
    removed.record.count(resp_excl, thing.to.say = paste0("Sequences taking longer than ",total.minutes.threshold," minutes to complete, removed: "))
  }
  if (!is.null(section.map)) {
    if (qbank == TRUE) {
      warning("Why do you have a section map for qbank")
      section.map.df <- data.frame(sectionName = unlist(section.map$jasperSectionName),
                                   test_minutes_allowed = section.map$minutes_allowed,
                                   test_response_threshold = section.map$response_threshold)
    } else {
      section.map.df <- section.map %>% rename(sectionName = jasperSectionName, section_response_threshold = sectionResponseThreshold)
    }
    resp_excl <- resp_excl %>% ## prep to find sequences with bad records in them, or too much time in a section, or multiple items seen in one test, or too many items in a section (repeated positions) for total exclusion
      merge(.,section.map.df) %>%
      dplyr::group_by(sequence_id_hist, sectionName) %>%
      dplyr::mutate(actual_num_ques = length(content_item_name)) %>%
      dplyr::ungroup()
  }
  if (!is.null(test.map) & qbank == FALSE) {
    resp_excl <- resp_excl %>% ## prep to find sequences with bad records in them, or too much time in a section, or multiple items seen in one test, or too many items in a section (repeated positions) for total exclusion
      merge(.,data.frame(template_name = test.map$template_name,
                         test_minutes_allowed = test.map$minutes_allowed,
                         test_num_ques = test.map$num_ques,
                         test_response_threshold = test.map$response_threshold,
                         strings_as_factors = FALSE)) %>%
      dplyr::group_by(sequence_id_hist, template_name) %>%
      dplyr::mutate(actual_num_ques = length(content_item_name)) %>%
      dplyr::ungroup()
  } else if (!is.null(test.map) & qbank == TRUE) {
    temp_record_check <- dim(resp_excl)[1]
    resp_excl <- resp_excl %>% ## prep to find sequences with bad records in them, or too much time in a section, or multiple items seen in one test, or too many items in a section (repeated positions) for total exclusion
      merge(.,data.frame(test_minutes_allowed = test.map$minutes_allowed,
                         test_num_ques = test.map$num_ques,
                         test_response_threshold = test.map$response_threshold,
                         strings_as_factors = FALSE)) %>%
      dplyr::group_by(sequence_id_hist, template_name) %>%
      dplyr::mutate(actual_num_ques = length(content_item_name)) %>%
      dplyr::ungroup()
    if (temp_record_check != dim(resp_excl)[1]) { stop("Too many things in test.map, probably")}
  }
  
  print("Here are the new columns after joining all the test and section maps")
  print(names(resp_excl)[!(names(resp_excl) %in% initial_columns)])
  if (remove.no.response.scored == TRUE) {
    seqHist.to.exclude.calc1 <- resp_excl %>%
      dplyr::filter(scored_response == 1 & raw_response == 0) %>% ## Excluding sequences that have weird response records - scored as correct without a response
      dplyr::ungroup() %>% dplyr::select(sequence_id_hist) %>% dplyr::distinct(sequence_id_hist)
    if (length(seqHist.to.exclude.calc1$sequence_id_hist) > 0) {
      resp_excl <- resp_excl %>%
        filter(!(sequence_id_hist %in% seqHist.to.exclude.calc1$sequence_id_hist))
    }
  removed.record.count(resp_excl, thing.to.say = "Sequences with bad records (raw_response = 0 with scored_response = 1), removed: ")
  }

  seqHist.to.exclude.calc1.5 <- resp_excl %>%
    dplyr::filter(milliseconds_used < 0) %>% ## EXCLUDING sequences that have a response with negative time
    dplyr::ungroup() %>% dplyr::select(sequence_id_hist) %>% dplyr::distinct(sequence_id_hist)
  if (length(seqHist.to.exclude.calc1.5$sequence_id_hist) > 0) {
    resp_excl <- resp_excl %>%
      filter(!(sequence_id_hist %in% seqHist.to.exclude.calc1.5$sequence_id_hist))
  }
  removed.record.count(resp_excl, thing.to.say = "Sequences with bad timing (milliseconds_used < 0), removed: ")

  # if (remove.over.time.sequences == TRUE) {
  #   if (!is.null(section.map)) {
  #     seqHist.to.exclude.calc2 <- resp_excl %>%
  #       dplyr::group_by(sequence_id_hist, sectionName, test_minutes_allowed) %>%
  #       dplyr::summarise(section_time = sum(milliseconds_used/60000)) %>% ## this gets the time in minutes
  #       dplyr::filter(section_time > test_minutes_allowed) %>% ## EXCLUDING all sequences where a section is over the number of minutes allowed
  #       dplyr::ungroup() %>% dplyr::select(sequence_id_hist) %>% dplyr::distinct(sequence_id_hist)
  #   } else if (!is.null(test.map)) {
  #     seqHist.to.exclude.calc2 <- resp_excl %>%
  #       dplyr::group_by(sequence_id_hist, test_minutes_allowed) %>%
  #       dplyr::summarise(test_sum_time = sum(milliseconds_used/60000)) %>% ## this gets the time in minutes
  #       dplyr::filter(test_sum_time > test_minutes_allowed) %>% ## EXCLUDING sequences over the number of minutes allowed
  #       dplyr::ungroup() %>% dplyr::select(sequence_id_hist) %>% dplyr::distinct(sequence_id_hist)
  #   }
  #   if (length(seqHist.to.exclude.calc2$sequence_id_hist) > 0) {
  #     resp_excl <- resp_excl %>%
  #       filter(!(sequence_id_hist %in% seqHist.to.exclude.calc2$sequence_id_hist))
  #   }
  # }
  # removed.record.count(resp_excl, thing.to.say = "Sequences (or sections) over the minutes allowed threshold, removed: ")

  if (qbank == FALSE) {
    seqHist.to.exclude.calc3 <- resp_excl %>%
      dplyr::filter(actual_num_ques > test_num_ques) %>% ## Excluding sequences that have more questions than they should (or sections)
      dplyr::ungroup() %>% dplyr::select(sequence_id_hist) %>% dplyr::distinct(sequence_id_hist)
    if (length(seqHist.to.exclude.calc3$sequence_id_hist) > 0) {
      resp_excl <- resp_excl %>%
        filter(!(sequence_id_hist %in% seqHist.to.exclude.calc3$sequence_id_hist))
    }
    removed.record.count(resp_excl, thing.to.say = "Sequences with too many questions in a section, removed: ")

  }

  seqHist.to.exclude.calc4 <- resp_excl %>%
    filter(content_item_id != -1) %>%
    group_by(sequence_id_hist, content_item_name) %>%
    summarise(count = length(content_item_name)) %>%
    filter(count > 1) %>% ## THIS IS THE REAL FILTER - Excluding sequences that have a single content item more than once (after filtering out tutorials/breaks/staged)
    dplyr::ungroup() %>% dplyr::select(sequence_id_hist) %>% dplyr::distinct(sequence_id_hist)
  if (length(seqHist.to.exclude.calc4$sequence_id_hist) > 0) {
    resp_excl <- resp_excl %>%
      filter(!(sequence_id_hist %in% seqHist.to.exclude.calc4$sequence_id_hist))
  }
  removed.record.count(resp_excl, thing.to.say = "Sequences with dupe content items within the same exam, removed: ")

  if (remove.repeat.test.administrations == TRUE) {
    seqHist.to.exclude.calc5 <- resp_excl %>%
      group_by(student_id, template_name, sequence_id_hist, timestamp_created) %>%
      summarise(num_ques = length(content_item_name)) %>%
      dplyr::ungroup() %>% dplyr::group_by(student_id, template_name) %>%
      mutate(sequence_order = dplyr::row_number(timestamp_created)) %>%
      filter(sequence_order > 1) %>% ## Excluding sequences that are not the first of their template administered to the user
      dplyr::select(sequence_id_hist) %>% dplyr::distinct(sequence_id_hist)
    if (length(seqHist.to.exclude.calc5$sequence_id_hist) > 0) {
      resp_excl <- resp_excl %>%
        filter(!(sequence_id_hist %in% seqHist.to.exclude.calc5$sequence_id_hist))
    }
    removed.record.count(resp_excl, thing.to.say = "Sequences that were not the first administration for the user, removed: ")
  }

  if (recode.answers) {
    if (class(resp_excl$raw_response) == "character"){
      resp_excl$rawest_response <- resp_excl$raw_response
      resp_excl$raw_response <- recode(resp_excl$raw_response, "A" = 1,"B" = 2, "C" = 3, "D" = 4)  
    }
  }
  

  if (!is.null(seqHist.to.exclude)) {
    resp_excl <- resp_excl %>%
      filter(!(sequence_id_hist %in% seqHist.to.exclude))

    num_seq_new <- length(unique(resp_excl$sequence_id_hist))
    num_users_new <- length(unique(resp_excl$student_id))
    cleaning_info <- print.if.verbose(paste0("Sequences input from list, removed: ", num_seq_current - num_seq_new), v, cleaning_info)
    cleaning_info <- print.if.verbose(paste0("Users removed: ", num_users_current - num_users_new), v, cleaning_info)
    num_seq_current <- num_seq_new
    num_users_current <- num_users_new
  }
  cleaning_info <- print.if.verbose(paste0("Current number of sequences: ", num_seq_current), v, cleaning_info)
  cleaning_info <- print.if.verbose(paste0("Current number of users: ", num_users_current), v, cleaning_info)
  cleaning_info <- print.if.verbose(paste0("Current number of items: ", num_items_current), v, cleaning_info)


  ## ITEM LEVEL EXCLUSIONS
  cleaning_info <- print.if.verbose(paste0("ITEM LEVEL EXCLUSIONS"), v, cleaning_info)
  num_item_responses <- dim(resp_excl)[1]
  cleaning_info <- print.if.verbose(paste0("Total item responses: ", num_item_responses), v, cleaning_info)

  ## create a column for attempted TRUE or FALSE based on responseStatus
  resp_excl <- resp_excl %>% dplyr::mutate(attempted = ifelse(is.na(raw_response), FALSE, raw_response != 0))

  resp_excl <- resp_excl %>% dplyr::filter(content_item_id != -1) ## these are staged records and do not represent a question that was viewed

  num_item_responses_new <- dim(resp_excl)[1]
  cleaning_info <- print.if.verbose(paste0("Staged response records removed: ", num_item_responses - num_item_responses_new), v, cleaning_info)
  num_item_responses <- num_item_responses_new

  if (remove.unscored == TRUE) { ## remove unscored items if requested, otherwise do nothing. defaults to doing nothing.
    resp_excl <- resp_excl %>% dplyr::filter(is_scored == 1)

    num_item_responses_new <- dim(resp_excl)[1]
    cleaning_info <- print.if.verbose(paste0("Unscored item responses removed: ", num_item_responses - num_item_responses_new), v, cleaning_info)
    num_item_responses <- num_item_responses_new
  }

  remove.value <- FALSE
  if (!(repeat.treatment %in% c("omit","remove","ignore"))) {
    message("Unknown repeat.treatment value. Allowed values include 'omit','remove', and 'ignore'. Repeated questions are recorded as omit by default.")
  } else if (repeat.treatment == "omit") {
    remove.value <- FALSE
  } else if (repeat.treatment == "remove") {
    remove.value <- TRUE
  } else if (repeat.treatment == "ignore") {
    remove.value <- NULL
  }
  cleaning_info <- print.if.verbose(paste0("remove repeat item responses, instead of recoding as omitted = ",remove.value), v, cleaning_info)
   num_items_omitted <- dim(resp_excl[!resp_excl$attempted,])[1]
  num_seq_w_omitted <- dim(unique(resp_excl[!resp_excl$attempted,"sequence_id_hist"]))[1]
  num_users_w_omitted <- dim(unique(resp_excl[!resp_excl$attempted,"student_id"]))[1]
  num_items_omitted_new <- 0
  num_seq_w_omitted_new <- 0
  num_users_w_omitted_new <- 0
  cleaning_info <- print.if.verbose(paste0("Original number of responses omitted: ", num_items_omitted), v, cleaning_info)
  cleaning_info <- print.if.verbose(paste0("Original number of seq w items omitted: ", num_seq_w_omitted), v, cleaning_info)
  cleaning_info <- print.if.verbose(paste0("Original number of users w items omitted: ", num_users_w_omitted), v, cleaning_info)

  if (!is.null(CI.old.version.dates)) {
    ## If items were under an earlier version the response should be recoded as omitted
    if (sum(names(CI.old.version.dates) == "content_item_name") > 0) {
        for (i in seq_along(CI.old.version.dates$content_item_name)) {
        resp_excl <- recode.as.omitted(resp_excl,
                                                       omit.condition = (resp_excl$content_item_name == CI.old.version.dates$content_item_name[i] &
                                                                           if (CI.remove.before.after == "before") {resp_excl$timestamp_created < CI.old.version.dates$cutoff_date[i]} else if (CI.remove.before.after == "after") {resp_excl$timestamp_created > CI.old.version.dates$cutoff_date[i]})
        )
      }
    } else if (sum(names(CI.old.version.dates) == "content_item_id") > 0) {
      for (i in seq_along(CI.old.version.dates$content_item_id)) {
        resp_excl <- recode.as.omitted(resp_excl,
                                                       omit.condition = (resp_excl$content_item_id == CI.old.version.dates$content_item_id[i] &
                                                                           resp_excl$timestamp_created < CI.old.version.dates$cutoff_date[i])
        )
      }
    }

    num_items_omitted_new <- dim(resp_excl[!resp_excl$attempted,])[1]
    num_seq_w_omitted_new <- dim(unique(resp_excl[!resp_excl$attempted,"sequence_id_hist"]))[1]
    num_users_w_omitted_new <- dim(unique(resp_excl[!resp_excl$attempted,"student_id"]))[1]
    cleaning_info <- print.if.verbose(paste0("Item responses under previous version marked as omitted: ", num_items_omitted_new - num_items_omitted), v, cleaning_info)
    cleaning_info <- print.if.verbose(paste0("Affected sequences : ", num_seq_w_omitted_new - num_seq_w_omitted), v, cleaning_info)
    cleaning_info <- print.if.verbose(paste0("Affected users : ", num_users_w_omitted_new - num_users_w_omitted), v, cleaning_info)
    num_items_omitted <- num_items_omitted_new
    num_seq_w_omitted <- num_seq_w_omitted_new
    num_users_w_omitted <- num_users_w_omitted_new
  }

  if (!is.null(CI.old.version.list)) {
    ## If items were under an earlier version the response should be recoded as omitted
    #browser()
    for (i in seq_along(CI.old.version.list$content_item_id)) {
      resp_excl <- recode.as.omitted(resp_excl,
                                                     omit.condition = (resp_excl$content_item_id == CI.old.version.list$content_item_id[i])
      )
    }
    num_items_omitted_new <- dim(resp_excl[!resp_excl$attempted,])[1]
    num_seq_w_omitted_new <- dim(unique(resp_excl[!resp_excl$attempted,"sequence_id_hist"]))[1]
    num_users_w_omitted_new <- dim(unique(resp_excl[!resp_excl$attempted,"student_id"]))[1]
    cleaning_info <- print.if.verbose(paste0("Item responses under previous version (from id list) marked as omitted: ", num_items_omitted_new - num_items_omitted), v, cleaning_info)
    cleaning_info <- print.if.verbose(paste0("Affected sequences : ", num_seq_w_omitted_new - num_seq_w_omitted), v, cleaning_info)
    cleaning_info <- print.if.verbose(paste0("Affected users : ", num_users_w_omitted_new - num_users_w_omitted), v, cleaning_info)
    num_items_omitted <- num_items_omitted_new
    num_seq_w_omitted <- num_seq_w_omitted_new
    num_users_w_omitted <- num_users_w_omitted_new
  }

  if (!is.null(remove.value)) {
    resp_excl <- remove.repeat.questions(resp_excl, remove = remove.value, add.col = TRUE)

    num_items_omitted_new <- dim(resp_excl[!resp_excl$attempted,])[1]
    num_seq_w_omitted_new <- dim(unique(resp_excl[!resp_excl$attempted,"sequence_id_hist"]))[1]
    num_users_w_omitted_new <- dim(unique(resp_excl[!resp_excl$attempted,"student_id"]))[1]
    cleaning_info <- print.if.verbose(paste0("Repeated items marked as omitted: ", num_items_omitted_new - num_items_omitted), v, cleaning_info)

    cleaning_info <- print.if.verbose(paste0("Current number of responses omitted: ", num_items_omitted_new), v, cleaning_info)
    cleaning_info <- print.if.verbose(paste0("Current number of seq w items omitted: ", num_seq_w_omitted_new), v, cleaning_info)
    cleaning_info <- print.if.verbose(paste0("Current number of users w items omitted: ", num_users_w_omitted_new), v, cleaning_info)
    # cleaning_info <- print.if.verbose(paste0("Affected sequences: ", num_seq_w_omitted_new - num_seq_w_omitted), v, cleaning_info)
    # cleaning_info <- print.if.verbose(paste0("Affected users: ", num_users_w_omitted_new - num_users_w_omitted), v, cleaning_info)
    num_items_omitted <- num_items_omitted_new
    num_seq_w_omitted <- num_seq_w_omitted_new
    num_users_w_omitted <- num_users_w_omitted_new


    marked_omit_items <- resp_excl %>% filter(raw_response != orig_response)
    write.csv(marked_omit_items, file.path(results_path, "Marked omitted repeat items.csv"), row.names = FALSE)

  }

  resp_excl <- timing.exclusion(resp_excl, mSec.min.threshold = mSec.min.threshold, sec.min.threshold = sec.min.threshold,
                                         mSec.max.threshold = mSec.max.threshold, sec.max.threshold = sec.max.threshold)
    ## Responses given in less than the threshold allowed will be recoded as omitted
  num_items_omitted_new <- dim(resp_excl[!resp_excl$attempted,])[1]
  num_seq_w_omitted_new <- dim(unique(resp_excl[!resp_excl$attempted,"sequence_id_hist"]))[1]
  num_users_w_omitted_new <- dim(unique(resp_excl[!resp_excl$attempted,"student_id"]))[1]
  cleaning_info <- print.if.verbose(paste0("Item response time under threshold of ",mSec.min.threshold," mSec or over ",mSec.max.threshold, " mSec, marked as omitted: ", num_items_omitted_new - num_items_omitted), v, cleaning_info)
  # cleaning_info <- print.if.verbose(paste0("Affected sequences: ", num_seq_w_omitted_new - num_seq_w_omitted), v, cleaning_info)
  # cleaning_info <- print.if.verbose(paste0("Affected users: ", num_users_w_omitted_new - num_users_w_omitted), v, cleaning_info)
  cleaning_info <- print.if.verbose(paste0("Current number of responses omitted: ", num_items_omitted_new), v, cleaning_info)
  cleaning_info <- print.if.verbose(paste0("Current number of seq w items omitted: ", num_seq_w_omitted_new), v, cleaning_info)
  cleaning_info <- print.if.verbose(paste0("Current number of users w items omitted: ", num_users_w_omitted_new), v, cleaning_info)
  num_items_omitted <- num_items_omitted_new
  num_seq_w_omitted <- num_seq_w_omitted_new
  num_users_w_omitted <- num_users_w_omitted_new


  if (!is.null(CI.old.keys)) {
    ## If items are scored from an earlier answer key the response should be recoded as omitted
    for (i in seq_along(CI.old.keys$content_item_id)) {
      resp_excl <- recode.as.omitted(resp_excl,
                                                     omit.condition = (resp_excl$content_item_id == CI.old.keys$content_item_id[i] &
                                                                       resp_excl$correctAnswer == CI.old.keys$correctAnswer[i])
                                                     )
    }
    num_items_omitted_new <- dim(resp_excl[!resp_excl$attempted,])[1]
    num_seq_w_omitted_new <- dim(unique(resp_excl[!resp_excl$attempted,"sequence_id_hist"]))[1]
    num_users_w_omitted_new <- dim(unique(resp_excl[!resp_excl$attempted,"student_id"]))[1]
    cleaning_info <- print.if.verbose(paste0("Item responses with previous version of answer key marked as omitted: ", num_items_omitted_new - num_items_omitted), v, cleaning_info)
    cleaning_info <- print.if.verbose(paste0("Affected sequences : ", num_seq_w_omitted_new - num_seq_w_omitted), v, cleaning_info)
    cleaning_info <- print.if.verbose(paste0("Affected users : ", num_users_w_omitted_new - num_users_w_omitted), v, cleaning_info)
    num_items_omitted <- num_items_omitted_new
    num_seq_w_omitted <- num_seq_w_omitted_new
    num_users_w_omitted <- num_users_w_omitted_new
  }


  removed.record.count(resp_excl, thing.to.say = "Number of sequences removed during item exclusions: ")

  if (precombined.files == FALSE) {
    ## ADD CONTENT ITEM INFO
    if (!is.null(cidf)) {
      if (!is.null(ci.cols.to.include)) {
        resp_excl <- combine.CIinfo(data_path, resp_excl,cidf = cidf, ci.cols.to.include = ci.cols.to.include, interaction.type.list = interaction.type.list)
      } else resp_excl <- combine.CIinfo(data_path, resp_excl, cidf = cidf, interaction.type.list = interaction.type.list)
    } else resp_excl <- combine.CIinfo(data_path, resp_excl, interaction.type.list = interaction.type.list)
    removed.record.count(resp_excl, thing.to.say = "Number of sequences removed during content item join (should be 0): ")
  }

  if (!is.null(field.test.items)) {
    resp_excl$FT <- resp_excl$content_item_name %in% field_test_items
  }



  cleaning_info <- print.if.verbose(paste0("Remaining number of sequences at this point: ", num_seq_current), v, cleaning_info)
  cleaning_info <- print.if.verbose(paste0("Remaining number of users at this point: ", num_users_current), v, cleaning_info)

    # Split into separate data frames for each section, or consider together if no section split
  if (!is.null(section.map) | section.separated == TRUE) {

    if (qbank == TRUE) {
      output.df.list <- vector("list", max(seq_along(section.map$jasperSectionName))+1) ## stage the list to have one more element than number of sections, the first element will hold the cleaning info
      # Sequences with less than a predetermined number of valid sequence responses in each of the sections (considered separately) will be excluded
      resp_excl <- resp_excl %>%
        group_by(student_id) %>% ## get calculations across entire pool of questions
        mutate(overall_raw_correct = sum(scored_response),
               overall_num_attempted = sum(attempted),
               overall_pTotal = overall_raw_correct/length(unique(resp_excl$content_item_name)), ## divide by total number of unique questions in this section
               overall_pPlus = overall_raw_correct/overall_num_attempted)
      resp_excl <- resp_excl %>%
        group_by(sequence_id_hist) %>% ## get all sequence level calculations
        mutate(template_raw_correct = sum(scored_response),
               template_num_attempted = sum(attempted),
               template_pTotal = template_raw_correct/actual_num_ques, ## total questions on a single exam across all sections
               template_pPlus = template_raw_correct/template_num_attempted)
      resp_excl <- resp_excl %>%
        group_by(sequence_id_hist, sectionName) %>% ## get all the calculations at the section level
        mutate(section_num_omitted = sum(!attempted),
               section_num_attempted = sum(attempted),
               section_perc_attempted = section_num_attempted/section_num_ques,
               section_raw_correct = sum(scored_response),
               section_num_scored = sum(scored),
               section_pTotal = section_raw_correct/section_num_ques,
               section_pPlus = section_raw_correct/section_num_attempted) %>%
        ungroup()
      if (is.null(min.items.per.seq)) {
        cleaning_info <- print.if.verbose("No minimum item threshold provided for this qbank.", v = v, cleaning_info)
      } else {
        seq.below.resp.threshold <- resp_excl %>%
          filter(template_num_attempted < min.items.per.seq)
        seq.below.resp.threshold <- seq.below.resp.threshold$sequence_id_hist ## overwrite with vector to reduce size
        resp_excl <- resp_excl %>%
          dplyr::filter(!(sequence_id_hist %in% seq.below.resp.threshold))
        print("finding the sequence order")
        seq_order_df <- resp_excl %>% ## calculate overall sequence order after all cleaning is complete - only needed here because of qbank
          ungroup() %>%
          select(student_id, sequence_id_hist, timestamp_created) %>%
          distinct() %>%
          group_by(student_id) %>%
          arrange(timestamp_created) %>%
          mutate(actual_sequence_order = dplyr::row_number(timestamp_created))
        resp_excl <- merge(resp_excl, seq_order_df)
        removed.record.count(resp_excl, thing.to.say = "Sequences removed under the threshold of attempted questions: ")
      }

      for (i in seq_along(section.map$jasperSectionName)) {
        output.df.list[[i+1]] <- resp_excl %>%
          dplyr::filter(sectionName == section.map$jasperSectionName[i])
        names(output.df.list)[i+1] <- section.map$jasperSectionName[i]
      }
        # names(output.df.list) <- section.map$section

        cleaning_info <- print.if.verbose(paste0("Remaining number of sequences in final output: ", num_seq_current), v, cleaning_info)
        cleaning_info <- print.if.verbose(paste0("Remaining number of users in final output: ", num_users_current), v, cleaning_info)
        cleaning_info <- print.if.verbose(paste0("Remaining number of unique items in final output: ", num_items_current), v, cleaning_info)
        cleaning_info <- print.if.verbose(paste0("Cleaning function time completed: ", Sys.time()), v, cleaning_info)

        output.df.list[[1]] <- cleaning_info
        names(output.df.list)[1] <- "cleaning_info"
        output.df.list
    } else { ## if this is NOT a qbank
      if (section.separated & !is.null(section.map)) {
        total_qs <- sum(section.map$section_num_ques)
        output.df.list <- vector("list", max(seq_along(section.map$jasperSectionName))+1) ## stage the list to have one more element than number of sections, the first element will hold the cleaning info
   # Sequences with less than a predetermined number of valid sequence responses in each of the sections (considered separately) will be excluded
      } else {
        if (!is.null(test.map)) {
          total_qs <- sum(test.map$numQues)
        } else warning("No test.map or section.map")
        output.df.list <- vector("list",2)
      }
   resp_excl <- resp_excl %>%
        group_by(student_id) %>% ## get calculations across entire pool of questions
        mutate(overall_raw_correct = sum(scored_response),
               overall_num_attempted = sum(attempted),
               overall_pTotal = overall_raw_correct/(length(unique(sequence_id_hist))*total_qs) , ## divide by total number of questions expected across all tests, this only works if tests are all the same
               overall_pPlus = overall_raw_correct/overall_num_attempted)
      resp_excl <- resp_excl %>%
        group_by(sequence_id_hist) %>% ## get all sequence level calculations
        mutate(template_raw_correct = sum(scored_response),
               template_num_attempted = sum(attempted),
               template_pTotal = template_raw_correct/test_num_ques, ## total questions on a single exam across all sections
               template_pPlus = template_raw_correct/template_num_attempted)
      resp_excl <- resp_excl %>%
        group_by(sequence_id_hist, sectionName) %>% ## get all the calculations at the section level
        mutate(section_num_omitted = sum(!attempted),
               section_num_attempted = sum(attempted),
               section_perc_attempted = section_num_attempted/section_num_ques,
               section_raw_correct = sum(scored_response),
               section_num_scored = sum(is_scored),
               section_pTotal = section_raw_correct/section_num_ques,
               section_pPlus = section_raw_correct/section_num_attempted) %>%
        ungroup()
      ## response threshold filter - first use section map if any, otherwise use response threshold
      # browser()
      if (!is.null(section.map$min.items.per.seq)) {
        seq.below.resp.threshold <- resp_excl %>%
          merge(.,data.frame(sectionName = unlist(section.map$jasperSectionName),
                             min.items.per.seq = section.map$min.items.per.seq)) %>%
          filter(section_num_attempted < min.items.per.seq | template_num_attempted < sum(section.map$min.items.per.seq))
        seq.below.resp.threshold <- seq.below.resp.threshold$sequence_id_hist ## overwrite with vector to reduce size

        resp_excl <- resp_excl %>%
          dplyr::filter(!(sequence_id_hist %in% seq.below.resp.threshold))
        removed.record.count(resp_excl, thing.to.say = "Sequences removed because one or more sections were under the threshold of attempted questions: ")
      } else if(section.calc == TRUE) {
        seq.below.resp.threshold <- resp_excl %>%
          filter(section_perc_attempted < section_response_threshold)
        seq.below.resp.threshold <- seq.below.resp.threshold$sequence_id_hist ## overwrite with vector to reduce size

        resp_excl <- resp_excl %>%
          dplyr::filter(!(sequence_id_hist %in% seq.below.resp.threshold))
        removed.record.count(resp_excl, thing.to.say = "Sequences removed because one or more sections were under the threshold of attempted questions: ")
      } else if(section.calc == FALSE) {
        seq.below.resp.threshold <- resp_excl %>%
          filter(section_perc_attempted < test_response_threshold) ## This should come from test.map, might not, idk
        seq.below.resp.threshold <- seq.below.resp.threshold$sequence_id_hist ## overwrite with vector to reduce size

        resp_excl <- resp_excl %>%
          dplyr::filter(!(sequence_id_hist %in% seq.below.resp.threshold))
        removed.record.count(resp_excl, thing.to.say = "Sequences removed because one or more sections were under the threshold of attempted questions: ")

      }
      if (!is.null(section.map) & section.separated) {
        for (i in seq_along(section.map$jasperSectionName)) {
          output.df.list[[i+1]] <- resp_excl %>%
            dplyr::filter(sectionName == section.map$jasperSectionName[i])
          names(output.df.list)[i+1] <- section.map$jasperSectionName[i]
        }
      } else {
        output.df.list[[2]] <- resp_excl
        names(output.df.list)[2] <- "cleaned_data"
      }
      # names(output.df.list) <- section.map$section

      cleaning_info <- print.if.verbose(paste0("Remaining number of sequences in final output: ", num_seq_current), v, cleaning_info)
      cleaning_info <- print.if.verbose(paste0("Remaining number of users in final output: ", num_users_current), v, cleaning_info)
      cleaning_info <- print.if.verbose(paste0("Cleaning function time completed: ", Sys.time()), v, cleaning_info)

      output.df.list[[1]] <- cleaning_info
      names(output.df.list)[1] <- "cleaning_info"
      output.df.list
    }
  } else if (!is.null(test.map)) {
    if(qbank == TRUE) {
      output.df.list <- vector("list", 2) ## qbank assumes only one test
      # Sequences with less than a predetermined number of valid sequence responses in each of the sections (considered separately) will be excluded
      resp_excl <- resp_excl %>%
        group_by(sequence_id_hist) %>% ## get all sequence level calculations
        mutate(template_raw_correct = sum(scored_response),
               template_num_attempted = sum(attempted))
      print("Sequence level sums complete")
      resp_excl <- resp_excl %>%
        mutate(template_pTotal = template_raw_correct/actual_num_ques, ## total questions on a single exam across all sections
               template_pPlus = template_raw_correct/template_num_attempted)
      print("Sequence level calcs complete")
      if (is.null(min.items.per.seq)) {
        cleaning_info <- print.if.verbose("No minimum item threshold provided for this qbank.", v = v, cleaning_info)
      } else {
        seq.below.resp.threshold <- resp_excl %>%
          filter(template_num_attempted < min.items.per.seq)
        seq.below.resp.threshold <- seq.below.resp.threshold$sequence_id_hist ## overwrite with vector to reduce size
        resp_excl <- resp_excl %>%
          dplyr::filter(!(sequence_id_hist %in% seq.below.resp.threshold))
        print("finding the sequence order")
        seq_order_df <- resp_excl %>% ## calculate overall sequence order after all cleaning is complete - only needed here because of qbank
          ungroup() %>%
          select(student_id, sequence_id_hist, timestamp_created) %>%
          distinct() %>%
          group_by(student_id) %>%
          arrange(timestamp_created) %>%
          mutate(actual_sequence_order = dplyr::row_number(timestamp_created))
        resp_excl <- merge(resp_excl, seq_order_df)
        removed.record.count(resp_excl, thing.to.say = "Sequences removed under the threshold of attempted items: ")
      }
      number_of_unique_CIs <- length(unique(resp_excl$content_item_name))
      resp_excl <- resp_excl %>%
        group_by(student_id) %>% ## get calculations across entire pool of questions
        mutate(overall_raw_correct = sum(scored_response),
               overall_num_attempted = sum(attempted))
      print("Overall level sums complete")
      resp_excl <- resp_excl %>%
        mutate(overall_pTotal = overall_raw_correct/number_of_unique_CIs, ## divide by total number of unique questions in this section
               overall_pPlus = overall_raw_correct/overall_num_attempted)
      print("Overall level calcs complete")
      print("finding the sequence order")
      seq_order_df <- resp_excl %>% ## calculate overall sequence order after all cleaning is complete - only needed here because of qbank
        ungroup() %>%
        select(student_id, sequence_id_hist, timestamp_created) %>%
        distinct() %>%
        group_by(student_id) %>%
        arrange(timestamp_created) %>%
        mutate(actual_sequence_order = dplyr::row_number(timestamp_created))
      resp_excl <- merge(resp_excl, seq_order_df)
      output.df.list[[2]] <- resp_excl
      names(output.df.list)[2] <- "cleaned_data"

      # names(output.df.list) <- section.map$section
      cleaning_info <- print.if.verbose(paste0("Remaining number of responses in final output: ", dim(resp_excl)[1]), v, cleaning_info)
      cleaning_info <- print.if.verbose(paste0("Remaining number of sequences in final output: ", num_seq_current), v, cleaning_info)
      cleaning_info <- print.if.verbose(paste0("Remaining number of users in final output: ", num_users_current), v, cleaning_info)
      cleaning_info <- print.if.verbose(paste0("Remaining number of unique items in final output: ", num_items_current), v, cleaning_info)
      cleaning_info <- print.if.verbose(paste0("Cleaning function time completed: ", Sys.time()), v, cleaning_info)

      output.df.list[[1]] <- cleaning_info
      names(output.df.list)[1] <- "cleaning_info"
      output.df.list
    } else if (qbank == FALSE) {

      # Sequences with less than a predetermined number of valid responses will be excluded
      resp_excl <- resp_excl %>%
        group_by(sequence_id_hist) %>%
        mutate(template_num_omitted = sum(!attempted),
               template_num_attempted = sum(attempted),
               template_perc_attempted = template_num_attempted/test_num_ques,
               template_raw_correct = sum(scored_response),
               template_num_attempted = sum(attempted),
               template_pTotal = template_raw_correct/test_num_ques,
               template_pPlus = template_raw_correct/template_num_attempted) %>%
        filter(template_perc_attempted >= test_response_threshold) %>%
        ungroup()
      removed.record.count(resp_excl, thing.to.say = "Number of sequences below response attempt threshold, removed: ")
      # resp_excl <- resp_excl %>%
      #   group_by(student_id) %>% ## get calculations across entire pool of questions
      #   mutate(overall_raw_correct = sum(scored_response),
      #          overall_num_attempted = sum(attempted),
      #          overall_pTotal = overall_raw_correct/length(unique(resp_excl$content_item_name)), ## divide by total number of unique questions in this section
      #          overall_pPlus = overall_raw_correct/overall_num_attempted)
      # seq_order_df <- resp_excl %>% ## calculate overall sequence order after all cleaning is complete - only needed here because of qbank
      #   ungroup() %>%
      #   select(student_id, sequence_id_hist, timestamp_created) %>%
      #   distinct() %>%
      #   group_by(student_id) %>%
      #   arrange(timestamp_created) %>%
      #   mutate(actual_sequence_order = dplyr::row_number(timestamp_created))
      # resp_excl <- merge(resp_excl, seq_order_df)

      cleaning_info <- print.if.verbose(paste0("Remaining number of sequences in final output: ", num_seq_current), v, cleaning_info)
      cleaning_info <- print.if.verbose(paste0("Remaining number of users in final output: ", num_users_current), v, cleaning_info)
      cleaning_info <- print.if.verbose(paste0("Remaining number of unique items in final output: ", num_items_current), v, cleaning_info)
      cleaning_info <- print.if.verbose(paste0("Cleaning function time completed: ", Sys.time()), v, cleaning_info)

      output.df.list <- list(cleaning_info, resp_excl)
      names(output.df.list) <- c("cleaning_info",analysis.name)

      output.df.list
    } else warning("Parameter qbank was not true or false? somehow?")
  } else warning("No section.map or test.map!")



}
