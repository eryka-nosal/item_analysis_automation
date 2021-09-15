library()


print.if.verbose <- function(thing.to.print, v = TRUE, thing.to.append = NULL) {
  if (v == TRUE) {
    print(thing.to.print)
  }
  
  if (!is.null(thing.to.append)) {
    return(paste0(thing.to.append,"\r\n",thing.to.print))
  }
}

describe.dupe.cor.ans <- function(df, use.content_item_name = FALSE) {
  if (use.content_item_name == TRUE) {
    cadf <- df %>%
      ungroup() %>%
      distinct(content_item_name, correct_answer) %>%
      filter(!is.na(correct_answer))
    
    if (length(unique(cadf$content_item_name)) < length(unique(df$content_item_name))) {
      ## if there are CIs that don't appear to have any correct answers, get all the unique responses scored as correct.
      ##Needed for OLP data, since there is no unpacking
      cadf <- df %>%
        filter(scored_response == 1) %>%
        distinct(content_item_name, raw_response)
      names(cadf)[2] <- "correct_answer"
    }
    
    dupe_itemIds <-
      unique(cadf[duplicated(cadf$content_item_name), "content_item_name"])
    if (length(dupe_itemIds) == 0) {
      stop("No duplicate correct answers.")
    }
    df_to_summarize <-
      df %>% filter(content_item_name %in% dupe_itemIds,scored_response== 1)
    outputdf <-
      df_to_summarize %>% group_by(content_item_name, response) %>% summarise(total =
                                                                              length(content_item_name))
    outputdf
  } else {
    ## use.content_item_name is FALSE
    cadf <- df %>%
      ungroup() %>%
      distinct(content_item_id, correct_answer) %>%
      filter(!is.na(correct_answer))
    
    if (length(unique(cadf$content_item_id)) < length(unique(df$content_item_id))) {
      ## if there are CIs that don't appear to have any correct answers, get all the unique responses scored as correct.
      ##Needed for OLP data, since there is no unpacking
      cadf <- df %>%
        filter(scored_response == 1) %>%
        distinct(content_item_id, response)
      names(cadf)[2] <- "correct_answer"
    }
    
    dupe_itemIds <-
      unique(cadf[duplicated(cadf$content_item_id),]$content_item_id)
    if (length(dupe_itemIds) == 0) {
      stop("No duplicate correct answers.")
    }
    df_to_summarize <-
      df %>% filter(content_item_id %in% dupe_itemIds,scored_response== 1)
    if (!is.null(df_to_summarize$timestamp_created)) {
      outputdf <- df_to_summarize %>%
        group_by(content_item_id, response) %>%
        summarise(
          total = length(content_item_id),
          min_date = min(timestamp_created, na.rm = TRUE),
          max_date = max(timestamp_created, na.rm = TRUE)
        )
    } else {
      outputdf <- df_to_summarize %>%
        group_by(content_item_id, response) %>%
        summarise(total = length(content_item_id))
    }
    outputdf
  }
}

get.item.cor.ans <- function(df, no.correct.answer = FALSE, use.content.item.name = FALSE) {
  if(use.content.item.name == TRUE) {
    if (no.correct.answer) {
      cadf <- df %>%
        dplyr::filter(scored_response == 1) %>%
        dplyr::ungroup() %>%
        dplyr::distinct(content_item_name, raw_response,rawest_response)
      names(cadf)[2] <- "correct_answer"
      names(cadf)[3] <- "correct_answer_rawest"
      if (length(unique(cadf$content_item_name)) < dim(cadf)[1]) {
        #print("Duplicate correct answers for some items.")
        print(describe.dupe.cor.ans(df, use.content_item_name = use.content_item_name))
        stop("Duplicate correct answers for some items.")
      }
      cadf
    } else {
      cadf <- df %>%
        dplyr::ungroup() %>%
        dplyr::distinct(content_item_name, correct_answer) %>%
        dplyr::filter(!is.na(correct_answer)) %>%
        dplyr::arrange(content_item_name)
      if (length(unique(cadf$content_item_name)) < length(unique(df[!is.na(df$correct_answer),]$content_item_name))) {
        ## making sure to degroup here, because otherwise dplyr will "helpfully" add back in the grouped columns like sequenceId
        cadf <- df %>%
          dplyr::filter(scored_response == 1) %>%
          dplyr::ungroup() %>%
          dplyr::distinct(content_item_name, raw_response)
        names(cadf)[2] <- "correct_answer"
      }
      if (length(unique(cadf$content_item_name)) < dim(cadf)[1]) {
        #print("Duplicate correct answers for some items.")
        print(describe.dupe.cor.ans(df, use.content_item_name = use.content_item_name))
        stop("Duplicate correct answers for some items.")
      }
      cadf
    }
  } else { ## use.content_item_name is FALSE
    if (no.correct.answer) {
      cadf <- df %>%
        dplyr::filter(scored_response == 1) %>%
        dplyr::ungroup() %>%
        dplyr::distinct(content_item_id, raw_response, rawest_response)
      names(cadf)[2] <- "correct_answer"
      names(cadf)[3] <- "correct_answer_rawest"
      if (length(unique(cadf$content_item_id)) < dim(cadf)[1]) {
        #print("Duplicate correct answers for some items.")
        print(describe.dupe.cor.ans(df, use.content_item_name = use.content_item_name))
        stop("Duplicate correct answers for some items.")
      }
      cadf
    } else {
      cadf <- df %>%
        dplyr::ungroup() %>%
        dplyr::distinct(content_item_id, correct_answer) %>%
        dplyr::filter(!is.na(correct_answer)) %>%
        dplyr::arrange(content_item_id)
      if (length(unique(cadf$content_item_id)) < length(unique(df[!is.na(df$correct_answer),]$content_item_id))) {
        ## making sure to degroup here, because otherwise dplyr will "helpfully" add back in the grouped columns like sequenceId
        cadf <- df %>%
          dplyr::filter(scored_response == 1) %>%
          dplyr::ungroup() %>%
          dplyr::distinct(content_item_id, raw_response)
        names(cadf)[2] <- "correct_answer"
      }
      if (length(unique(cadf$content_item_id)) < dim(cadf)[1]) {
        #print("Duplicate correct answers for some items.")
        print(describe.dupe.cor.ans(df, use.content_item_name = use.content_item_name))
        stop("Duplicate correct answers for some items.")
      }
      cadf
    }
  }
}


removed.record.count <- function(df, thing.to.say = "Sequences removed: " , include.item.count = FALSE) {
  ## get the new counts, which we'll compare to the "current" counts in the environment in which the function was called, parent.frame()
  num_seq_new <- length(unique(df$sequence_id_hist))
  num_users_new <- length(unique(df$student_id))
  num_items_new <- length(unique(df$content_item_name))
  ## add stuff to the cleaning info object - this only works if there is an object in the parent environment called cleaning_info
  assign("cleaning_info",
         value = print.if.verbose(paste0(thing.to.say, ## defaults to sequences removed
                                         parent.frame()$num_seq_current - num_seq_new),
                                  v = parent.frame()$v,
                                  parent.frame()$cleaning_info),
         envir = parent.frame())
  assign("cleaning_info",
         value = print.if.verbose(paste0("Users removed: ",
                                         parent.frame()$num_users_current - num_users_new),
                                  v = parent.frame()$v,
                                  parent.frame()$cleaning_info),
         envir = parent.frame())
  assign("cleaning_info",
         value = print.if.verbose(paste0("Unique items removed: ",
                                         parent.frame()$num_items_current - num_items_new),
                                  v = parent.frame()$v,
                                  parent.frame()$cleaning_info),
         envir = parent.frame())
  ## reassign these new counts to be current in the calling environment
  assign("num_seq_current", num_seq_new, envir = parent.frame())
  assign("num_users_current", num_users_new, envir = parent.frame())
  assign("num_items_current", num_items_new, envir = parent.frame())
}



recode.as.omitted <- function(df, omit.condition = NULL) {
  if (is.null(omit.condition)) {
    warning("No omit.condition provided.")
  }
  if (sum(names(df) %in% c("orig_response","orig_score")) == 0) {
    ## if there are no columns called orig_response or orig_score, make them before changing any data
    df <- df %>%
      dplyr::mutate(orig_response = raw_response,
                    orig_score = scored_response)
  }
  if (sum(is.na(omit.condition)) > 0) {
    omit.condition[is.na(omit.condition)] <- FALSE
    warning("Null omit.condition defaulted to FALSE")
  }
  if (length(omit.condition) > 0) {
    ## change responses to 0
    df[omit.condition,"raw_response"] <- 0
    ## change scored_response to 0
    df[omit.condition,"scored_response"] <- 0
    ## change attempted to FALSE
    df[omit.condition,"attempted"] <- FALSE
  } else warning("Nothing recoded due to entirely FALSE omit.condition.")
  
  
  df
}

timing.exclusion <- function(df, mSec.min.threshold = NULL, mSec.max.threshold = NULL, sec.min.threshold = NULL, sec.max.threshold = NULL) {
  
  df_time <- dplyr::filter(df, raw_response!=0)
  

  if (is.null(mSec.min.threshold) & is.null(sec.min.threshold) & is.null(mSec.max.threshold) & is.null(sec.max.threshold)) {
    ## this whole first part is only if arguments are not specified.
    ## When called by another function, one of the thresholds should always be specified.
    if (sum(grepl("milliseconds_used", names(df_time))) > 0) {
      print("Here's what the distribution of timing looks like in milliseconds for questions answered:")
      print(summary(df_time$milliseconds_used))
      print(paste0("There are ",dim(dplyr::filter(df_time,milliseconds_used<=0))[1]," actual responses where time spent was 0 milliseconds or less."))
      minTime <- as.numeric(readline("What is the minimum time allowed in milliseconds? "))
      print(paste0("This will change ",dim(df[df$milliseconds_used <= minTime,])[1]," responses to 'omit' status."))
      cont <- readline("Continue? Y/N ")
      
      if (cont == 'y'|cont == 'Y') {
        df <- recode.as.omitted(df, omit.condition = df$milliseconds_used <= minTime)
      } else print("No responses changed.")
      
    } else if (sum(grepl("seconds_used", names(df_time))) > 0) {
      print("Here's what the distribution of timing looks like in seconds for questions answered:")
      print(summary(df_time$seconds_used))
      print(paste0("There are ",dim(dplyr::filter(df_time,seconds_used<=0))[1]," actual responses where time spent was 0 seconds or less."))
      minTime <- as.numeric(readline("What is the minimum time allowed in seconds? "))
      print(paste0("This will change ",dim(df[df$seconds_used <= minTime,])[1]," responses to 'omit' status."))
      cont <- readline("Continue? Y/N ")
      
      if (cont == 'y'|cont == 'Y') {
        df <- recode.as.omitted(df, omit.condition = df$seconds_used <= minTime)
      } else print("No responses changed.")
      
    } else print("no time field found")
    
  } else if (!is.null(mSec.min.threshold) & sum(grepl("milliseconds_used", names(df_time))) > 0) {
    df <- recode.as.omitted(df, omit.condition = df$milliseconds_used <= mSec.min.threshold)
  } else if (!is.null(sec.min.threshold) & sum(grepl("seconds_used", names(df_time))) > 0) {
    df <- recode.as.omitted(df, omit.condition = df$seconds_used <= sec.min.threshold)
  } else if (!is.null(mSec.min.threshold)) {
    warning("Data frame does not contain milliseconds_used column. No questions recoded for minimum timing.")
  } else if (!is.null(sec.min.threshold)) {
    warning("Data frame does not contain seconds_used column. No questions recoded for minimum timing.")
  }
  
  if (!is.null(mSec.max.threshold) & sum(grepl("milliseconds_used", names(df_time))) > 0) {
    df <- recode.as.omitted(df, omit.condition = df$milliseconds_used > mSec.max.threshold)
  } else if (!is.null(sec.max.threshold) & sum(grepl("seconds_used", names(df_time))) > 0) {
    df <- recode.as.omitted(df, omit.condition = df$seconds_used > sec.max.threshold)
  } else if (!is.null(mSec.max.threshold)) {
    warning("Data frame does not contain milliseconds_used column. No questions recoded for minimum timing.")
  } else if (!is.null(sec.max.threshold)) {
    warning("Data frame does not contain seconds_used column. No questions recoded for minimum timing.")
  }
  
  df
  
}


remove.repeat.questions <- function(df, remove = FALSE, add.col = FALSE) {
  df <- df %>%
    dplyr::ungroup() %>%
    # order by the times the person saw the question - dates on sequences
    dplyr::group_by(content_item_id,student_id) %>%
    dplyr::mutate(ques_rank = dplyr::row_number(timestamp_created))
  
  if (remove == FALSE) {
    df <- recode.as.omitted(df, omit.condition = df$ques_rank > 1)
    if (add.col == TRUE) {
      if (sum(names(df) == "repeatOmitted") == 0) { ## if the column doesn't exist, create it
        df$repeatOmitted = df$ques_rank > 1
      }
    }
    outputdf <- df %>%
      dplyr::select(-ques_rank)
  } else if (remove == TRUE) {
    removed <- df %>%
      dplyr::filter(df$ques_rank > 1)
    df <- df %>%
      dplyr::filter(df$ques_rank == 1)
    
    outputdf <- list("output" = df %>% dplyr::select(-ques_rank),
                     "removed" = removed %>% dplyr::select(-one_of(c("ques_rank", cols_to_add)))
    )
  }
  
  outputdf
}

combine.CIinfo <- function(path, respdf, cidf = NULL, ci.cols.to.include = NULL, interaction.type.list = 1) {
  ## path cleanup
  path <- fix.path(path)
  
  ## read in content info file
  if(is.null(cidf)) {
    cidf <- read.anything(path,'contentItemInfo')
  }
  ## turn all initial letters in column headers lowercase
  names(cidf) <- gsub("^([A-Z])","\\L\\1",names(cidf),perl=TRUE)
  
  cidf <- unique(cidf)
  
  cidf <- cidf[cidf$interactionTypeId %in% interaction.type.list,]
  
  columnsToAdd <- c("content_item_name")
  columnsToAdd <- c(columnsToAdd,ci.cols.to.include)
  columnsdf <- cidf[, c("content_item_id",columnsToAdd)]
  outputdf <- merge(respdf, columnsdf)
  
  
} # end combine.CIinfo function




make.matrices <- function(df,
                          vars_for_matrices,
                          matrix_names,
                          destination_file_path,
                          destination_file_name_prefix,
                          cleaning_output,
                          omit_code = 5, not_seen_code = 6,
                          use_display_order = FALSE,
                          zero_sec_as_not_reached = FALSE) {
  
  if(zero_sec_as_not_reached == TRUE) {
    df <- df %>%
      filter(milliseconds_used > 0)
  }
  
  if(use_display_order == TRUE){
    level_df <- df %>%
      select(template_name, content_item_name, item_section_position) %>% distinct() %>%
      arrange(template_name, item_section_position)
    
    #EN: For BAR pilot: One content_item_name may be in two different template_names, so take only unique content_item_names
    #level_df <- df %>%
    #  select(content_item_name, item_section_position) %>% distinct() %>%
    #  arrange(item_section_position)
    
    content_item_vector <- factor(df$content_item_name, levels = unique(level_df$content_item_name))
  } else {
    content_item_vector <- df$content_item_name
  }
  
  
  ## initialize answer and seen matrices -----------------------------------------
  ans_pool_matrix <- reshape2::acast(df,
                                     df$student_id ~ content_item_vector,
                                     sum,
                                     value.var = "attempted")
  
  seen_pool_matrix <- reshape2::acast(df,
                                      df$student_id ~ content_item_vector,
                                      length)
  ## loop through all variables to create matrices -------------------------------
  for (i in seq_along(vars_for_matrices)) {
    big_matrix <- reshape2::acast(df,
                                  df$student_id ~ content_item_vector,
                                  sum,
                                  value.var = vars_for_matrices[i])
    spaces <- max(nchar(vars_for_matrices)) - nchar(vars_for_matrices[i])
    cleaning_output <- print.if.verbose(paste0("Distribution of ", vars_for_matrices[i], ": ", paste(rep(" ",spaces),collapse = ""),
                                               paste(summary(as.vector(big_matrix)),collapse = ", ")), v = v, cleaning_output)
    cleaning_output <- print.if.verbose(paste0("Size of ", vars_for_matrices[i], " matrix: ", paste(rep(" ",spaces),collapse = ""),
                                               paste(dim(big_matrix),collapse = " by ")), v = v, cleaning_output)
    if (vars_for_matrices[i] %in% c("raw_response","scored_response")) {
      big_matrix[ans_pool_matrix == 0] <- omit_code  ## replace all omits with given
      big_matrix[seen_pool_matrix == 0] <- not_seen_code  ## replace all not reached/seen with code 6 - this overwrites most of the 5s from the previous line
      
      # } else if (vars_for_matrices[i] == "scored_response") {
      #   big_matrix[ans_pool_matrix == 0] <- "."
    } else {
      big_matrix[seen_pool_matrix == 0] <- "."
    }
    write.csv(big_matrix, file.path(destination_file_path,paste0(destination_file_name_prefix,matrix_names[i],".csv")),row.names=T)
  }
  return(cleaning_output)
}



