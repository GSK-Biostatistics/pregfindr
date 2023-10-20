# Note: To split data, it is more efficient to group_split by column and save an intermediate object instead of filtering twice
# Unit: milliseconds
# expr      min       lq     mean   median       uq      max neval cld
# filter 277.0453 298.7826 327.2725 317.7338 348.1973 511.7599    50   b
# group_split 144.3497 150.3439 177.7069 171.0592 189.8519 269.1994    50  a

# Reading and writing data -------------
#' Save an R object as a Parquet file
#'
#' This function saves an R object (e.g., a data frame) as a Parquet file using the arrow package.
#' The file is saved in the specified directory with the object's name as the file name.
#'
#' @param obj The R object to be saved as a Parquet file. This is typically a data frame.
#' @param path The directory where the Parquet file will be saved. Defaults to the current working directory.
#' @param save logical, if FALSE object is returned without saving
#' @return NULL. The function prints a message indicating the file has been saved.
#' @examples
#' my_dataframe <- data.frame(a = 1:5, b = letters[1:5])
#' save_parquet(my_dataframe)
#' @export
#' @importFrom arrow write_parquet
#' @keywords internal
save_parquet <- function(obj, path = ".", save = FALSE) {
  if (save == TRUE) {
    if (nchar(system.file(package = "arrow")) == 0) {
      warning("Package `feather` is not installed. Data cannot be saved.")
    }
  # Get the name of the object
  obj_name <- deparse(substitute(obj))
  
  # Create the file path with the object name
  file_path <- file.path(path, paste0(obj_name, ".parquet"))
  
  # Save the object as a parquet file using arrow::write_parquet()
  arrow::write_parquet(obj, file_path)
  
  # Print a message to indicate the file has been saved
  cat("File saved as:", file_path, "\n")
  }
  return(obj) # so that it can be used in a pipe
}

#' Save table to Impala and return remote table
#'
#' @param connection Database connection
#' @param schema Database schema
#' @param tablename string
#' @param object_to_save table
#' @param overwrite logic, TRUE overwrites table in database, FALSE simply reads
#'
#' @return remote table
#' @export
#'
write_to_impala_then_read_lazy_table <- function(tablename,
                                                 object_to_save,
                                                 connection = NULL,
                                                 schema = NULL,
                                                 overwrite = TRUE) {
  # Write table to Impala
  if (overwrite == TRUE) {
    dbWriteTable(connection,
      Id(schema = schema, table = tablename),
      object_to_save,
      overwrite = TRUE
    )
  }
  # Read table lazily
  connection %>%
    tbl(in_schema(schema, tablename))
}


# Data extraction -------------

#' ID women that fit inclusion criteria of age and time period
#' @param .data MarketScan dataset
#' @param .variables_to_keep Variable names that should appear in the returned dataset
#' @param .age_start int, lower age boundary for inclusion
#' @param .age_end int, upper age boundary for inclusion
#' @param .last_yr int, study end year. Last year of full data on MarketScan dataset
#' @param .presvcdate POSIXct, start of study period minus a certain number of days to capture pregnancy start dates
#' @param .svcend POSIXct, end of study period
#' @param .claims_file chr, "I" for inpatient or "O" for outpatient
#'
#' @return Subset dataset
#' @export
#'
filter_eligible_women <- function(.data,
                                  .variables_to_keep,
                                  .age_start,
                                  .age_end,
                                  .last_yr,
                                  .presvcdate,
                                  .svcend,
                                  .claims_file) {
  # The same variable has name "rx" in MarketScan Commercial Claims and "drugcovg" in MarketScan Medicaid
  if(any(colnames(.data) == "drugcovg")) .data <- .data %>% rename(rx = drugcovg)

  .data %>% # data cut
    filter(
      year - dobyr >= .age_start,
      # aged at or greater than minimum age
      year - dobyr <= .age_end,
      # aged at or less than maximum age
      rx == "1",
      # have pharmacy benefit
      year <= .last_yr,
      # in years of interest
      sex == "2",
      # females only
      svcdate >= .presvcdate,
      # number of days before svcstart to capture pregnancy start dates
      svcdate <= .svcend, # svcdate during study period
      !is.null(enrolid)
    ) %>%
    # Specify where enrolid is coming from
    mutate(claims_file = .claims_file) %>%
    # select unique combinations of these variables (so we exclude 100% duplicate rows)
    select({{ .variables_to_keep }}) %>%
    distinct()
}


#' Go through the various "diagnosis" fields in the dataset, looking for matches with the ICD codes
#'
#' @param .data MarketScan dataset
#' @param inpat logical, for inpat or outpat claims
#' @param .dxver procedure code version
#' @param .icd_data dataset with icd codes
#' @param .icd_value icd value to match
#' @param .variables_to_keep Variable names that should appear in the returned dataset
#'
#' @return Dataset
#' @export
#'
find_matches_in_ICD <- function(.data, inpat, .dxver, .icd_data, .icd_value, .variables_to_keep) {
  # If the data is about inpatient claims, use pdx
  if (inpat == TRUE) {
    .data %>% # Take all eligible women
      filter(dxver == .dxver | is.na(dxver)) %>%
      {
        union(
          union(
            union(
              inner_join(., .icd_data, by = c("dx1" = .icd_value)) %>% rename(dx = dx1) %>% mutate(pdx = 1),
              inner_join(., .icd_data, by = c("dx2" = .icd_value)) %>% rename(dx = dx2) %>% mutate(pdx = 0)
            ),
            union(
              inner_join(., .icd_data, by = c("dx3" = .icd_value)) %>% rename(dx = dx3) %>% mutate(pdx = 0),
              inner_join(., .icd_data, by = c("dx4" = .icd_value)) %>% rename(dx = dx4) %>% mutate(pdx = 0)
            )
          ),
          inner_join(., .icd_data, by = c("pdx" = .icd_value)) %>% rename(dx = pdx) %>% mutate(pdx = 0)
        )
      } %>%
      # Retain columns of interest
      select({{ .variables_to_keep }})
  } else {
    .data %>% # Take all eligible women
      filter(dxver == .dxver | is.na(dxver)) %>%
      {
        union(
          union(
            inner_join(., .icd_data, by = c("dx1" = .icd_value)) %>% rename(dx = dx1) %>% mutate(pdx = 1),
            inner_join(., .icd_data, by = c("dx2" = .icd_value)) %>% rename(dx = dx2) %>% mutate(pdx = 0)
          ),
          union(
            inner_join(., .icd_data, by = c("dx3" = .icd_value)) %>% rename(dx = dx3) %>% mutate(pdx = 0),
            inner_join(., .icd_data, by = c("dx4" = .icd_value)) %>% rename(dx = dx4) %>% mutate(pdx = 0)
          )
        )
      } %>%
      # Retain columns of interest
      select({{ .variables_to_keep }})
  }
}

#' Keep and return records of eligible women that match pregnancy procedure codes with additional info
#'
#' @param .data MarketScan dataset
#' @param .proc_data Procedure code dataset
#' @param .by_var_claims Variable in claims data to join by
#' @param .by_var_proc_code Variable in procedure code dataset to join by
#' @param .variables_to_keep Variable names that should appear in the returned dataset
#'
#' @return Modified dataset
#' @export
#'
match_on_pregnancy_procedure_code <- function(.data, .proc_data, .by_var_claims, .by_var_proc_code, .variables_to_keep) {
  # Inner join needs named character vector
  assertthat::assert_that(length(.by_var_claims) == length(.by_var_proc_code))
  key <- setNames(.by_var_proc_code, .by_var_claims)
  # Matching on pregnancy codes
  inner_join(x = .data, y = .proc_data, by = key) %>%
    rename(proc = pproc) %>%
    select({{ .variables_to_keep }})
}

#' Keep and return records of eligible women that match CPT pregnancy codes with additional info
#'
#' @param .data MarketScan dataset
#' @param .cpt_data CPT procedure codes dataset
#' @param .by_var_claims Variable in claims data to join by
#' @param .by_var_cpt_code Variable in CPT procedure codes dataset to join by
#' @param .variables_to_keep Variable names that should appear in the returned dataset
#'
#' @return Modified dataset
#' @export
#'
match_on_pregnancy_cpt_code <- function(.data, .cpt_data, .by_var_claims = "proc1", .by_var_cpt_code = "cpt_char", .variables_to_keep) {
  # Inner join needs named character vector
  assertthat::assert_that(length(.by_var_claims) == length(.by_var_cpt_code))
  key <- setNames(.by_var_cpt_code, .by_var_claims)
  # Matching on pregnancy codes
  .data %>%
    inner_join(.cpt_data,
      by = key
    ) %>%
    select(-cpt) %>% # to prevent error when we rename proc1
    rename(
      description = cpt_desc,
      cpt = proc1
    ) %>%
    select({{ .variables_to_keep }})
}

#' Calculate additional age variables
#'
#' @param .data Combined ICD9/ICD10, PROC, and CPT as a single dataset
#'
#' @return Modified dataset
#' @export
#'
calculate_age_variables <- function(.data) {
  .data %>%
    mutate(
      svcdate = as.Date(svcdate, format = "%Y-%m-%d"),
      srv_year = lubridate::year(svcdate)
    )
}

# Other helpers ------------

#' Drop NAs helper
#'
#' @param trimester_data trimester_data
#' @import dplyr
#' @import dtplyr
#' @return filtered trimester_data
#'
drop_NAs_in_trimester_code_clean <- function(trimester_data) {
  lazy_dt(trimester_data) %>%
    filter(!is.na(trimester_code_clean)) %>%
    as_tibble()
}

#' Keep NAs helper
#'
#' @param trimester_data trimester_data
#' @import dplyr
#' @import dtplyr
#' @return filtered trimester_data
#'
keep_only_NAs_in_trimester_code_clean <- function(trimester_data) {
  lazy_dt(trimester_data) %>%
    filter(is.na(trimester_code_clean)) %>%
    as_tibble()
}

#' Calculate Nsundays (Number of Sundays per week)
#'
#' @param date1,date2 Dates
#'
#' @return Number of Sundays between dates
#'
Nsundays <- Vectorize(function(a, b) {
  sum(weekdays(seq(as.Date(a) + 1, as.Date(b), "day")) %in% c("Sunday"))
})


#' Helper for logging parameter name and value
#' This function takes in one or more atomic variables and converts them into a string format for logging purposes.
#' @param ... Object name of an atomic object
#'
#' @return A string representation of the input variables
#' @export
#'
vars_content_to_string <- function(...) {
  vars <- list(...)
  var_names <- sapply(as.list(match.call())[-1], deparse)
  var_strings <- character(length(vars))
  for (i in seq_along(vars)) {
    if (is.vector(vars[[i]]) && length(vars[[i]]) > 1) {
      var_strings[i] <- paste0(var_names[i], " = c(", paste(vars[[i]], collapse = ", "), ")")
    } else {
      var_strings[i] <- paste0(var_names[i], " = ", vars[[i]])
    }
  }
  return(paste(var_strings, collapse = "\n"))
}

#' Generate a validation error message
#'
#' This function generates a validation error message based on a list of errors.
#' If a data frame is provided and it has attached errors, these errors will be included in the message.
#' The message will include the total number of errors and the details of each error.
#'
#' @param list_of_errors A list of errors to include in the message
#' @param data An optional data frame that may have attached errors
#' @param ... Additional arguments (not used)
#' @return No return value; the function outputs the error message
validation_error <- function(list_of_errors, data = NULL, ...) {
  # we are checking to see if there are any errors that
  # are still attached to the data.frame
  if (!is.null(data) && !is.null(attr(data, "assertr_errors"))) {
    errors <- append(attr(data, "assertr_errors"), errors)
  }

  num.of.errors <- length(list_of_errors)

  preface <- sprintf(
    "There %s %d error%s:\n",
    ifelse(num.of.errors == 1, "is", "are"),
    num.of.errors,
    ifelse(num.of.errors == 1, "", "s")
  )
  error_string <- lapply(list_of_errors, function(x) paste(x[["message"]], collapse = "\n"))
  error_string <- c(preface, error_string)
  error_string <- paste(error_string, collapse = "\n")
  put(error_string)
}

#' Output a success confirmation message
#'
#' This function outputs a success confirmation message. It is intended to be used after a step in a process has been completed successfully.
#'
#' @param data An optional data frame (not used)
#' @return No return value; the function outputs the success confirmation message
success_confirmation <- function(data = NULL) {
  put("Step completed successfully.")
}
