#' Apply Outcome Hierarchy to Patients
#'
#' This function applies a series of outcome hierarchies to patient trimester data.
#' It combines outcomes for patients, adds outcome flags, and applies five levels of outcome hierarchy logic.
#' The final outcome hierarchy is returned as a data frame with distinct records, sorted by enrolid.
#'
#' @param trimester_hierarchy_final A data frame containing patient data with applied trimester hierarchy.
#' @return A data frame with the applied outcome hierarchy logic, containing distinct records sorted by enrolid.
#' @examples
#' \dontrun{
#' trimester_hierarchy_final <- apply_trimester_hierarchy(patients)
#' outcome_hierarchy_final <- apply_outcome_hierarchy(trimester_hierarchy_final)
#' }
#' @export
apply_outcome_hierarchy <- function(trimester_hierarchy_final) {
  tryCatch({  
  # Combine outcomes for Commercial or Medicaid Patients
  patient_outcomes_flag <- combine_outcomes(trimester_hierarchy_final) %>%
    add_outcome_flags()
  
  # Apply Outcome Hierarchy #1
  outcome_hierarchy1 <- apply_outcome_hierarchy_1(patient_outcomes_flag)
  
  # Apply Outcome Hierarchy #2
  outcome_hierarchy2 <- apply_outcome_hierarchy_2(outcome_hierarchy1, labor_delivery_codes)
  
  # Apply Outcome Hierarchy #3
  outcome_hierarchy3_list <- apply_outcome_hierarchy_3(outcome_hierarchy2)
  
  # Apply Outcome Hierarchy #4
  outcome_hierarchy4 <- apply_outcome_hierarchy_4(outcome_hierarchy3 = outcome_hierarchy3_list[["outcome_hierarchy3"]],
                                                  outcome_hierarchy3_nondups = outcome_hierarchy3_list[["outcome_hierarchy3_nondups"]],
                                                  multiple_lt_180_days = outcome_hierarchy3_list[["multiple_lt_180_days"]])
  # Apply Outcome Hierarchy #5
  outcome_hierarchy5 <- apply_outcome_hierarchy_5(outcome_hierarchy4)
  
  # Get final outcome hierarchy
  outcome_hierarchy_final <- outcome_hierarchy5 %>%
    distinct() %>%
    arrange(enrolid)
  
  # Garbage collection -------
  gc()
  return(outcome_hierarchy_final)
  
},
error = function(e) {
  message("The following errors are likely due to an insufficient record sample size. Consider broadening the age range of interest or extending the study duration.")
  stop(e)
})
}

# Pregnancy outcome hierarchy helpers -------------

#' Combine Outcomes
#'
#' This function combines outcomes from the trimester_hierarchy_final dataset.
#' It creates two separate datasets based on the presence of `outcome` and `secondary_outcome` variables,
#' and then binds them together into a single dataset.
#'
#' @param trimester_hierarchy_final A dataset containing trimester hierarchy information.
#' @return A combined dataset containing outcomes.
#' @noRd
combine_outcomes <- function(trimester_hierarchy_final) {
  outcome1_pt <- trimester_hierarchy_final %>%
    filter(!is.na(outcome)) %>%
    select(enrolid, svcdate, code, claims_file, description, final_trimester_code, srv_year, outcome, pdx, trimester_code_clean) %>%
    calculate_age_variables()
  
  
  outcome2_pt <- trimester_hierarchy_final %>%
    filter(!is.na(secondary_outcome) & (secondary_outcome != "")) %>%
    mutate(outcome = secondary_outcome) %>%
    calculate_age_variables() %>%
    select(enrolid, svcdate, code, claims_file, description, final_trimester_code, srv_year, pdx, trimester_code_clean, outcome)
  
  outcome_pt <- bind_rows(outcome1_pt, outcome2_pt)
}

#' Add Outcome Flags
#'
#' This function adds outcome flags (newborn_flag, multiple_flag, and loss_flag) to the input dataset.
#' The flags are based on the description and outcome variables in the dataset.
#'
#' @param .data A dataset containing outcomes.
#' @return A dataset with added outcome flags.
#' @noRd
add_outcome_flags <- function(.data) {
  outcome_pt_flag <- .data %>%
    mutate(
      newborn_flag =
        # might need to add other "new born" patterns in the future, however results are accurate as per the below logic
        if_else(str_detect(description, regex("newborn", ignore_case = TRUE)), 1, 0),
      multiple_flag =
        if_else(outcome %in% c(
          "Twin birth", "Twin birth*", "Multiple birth",
          "Multiple birth unspecified*", "Multiple birth unspecified", "Multiple birth*"
        ), 1, 0),
      loss_flag =
        if_else(outcome %in% c("Ectopic pregnancy", "Unclassified pregnancy loss", "Spontaneous abortion", "Induced abortion", "Stillbirth"), 1, 0)
    )
}

#' Apply Outcome Hierarchy #1
#'
#' This function applies a hierarchy to the given patient_outcomes_flag dataset.
#' The hierarchy is based on the following rules:
#' 1. If outcome of "Twin Birth" and "Twin Birth*", overwrite "Twin Birth*", as "Twin Birth" for entire pregnancy.
#' 2. If outcome of "Multiple Birth Unspecified" and "Multiple Birth Unspecified *", overwrite "Multiple Birth Unspecified *", as "Multiple Birth Unspecified" for entire pregnancy.
#' 3. If outcome of "Multiple Birth" and "Multiple Birth*", overwrite "Multiple Birth*", as "Multiple Birth" for entire pregnancy.
#' 4. If outcome of "Twin Birth*" and "Multiple Birth Unspecified*", overwrite "Multiple Birth Unspecified*" as "Twin Birth*" for entire pregnancy.
#' 5. Any non-stars use non-star; when both stars, use stars.
#' 6. If outcome of "Twin Birth*" and "Multiple Birth Unspecified*", overwrite "Multiple Birth Unspecified*" as "Twin Birth*" for entire pregnancy.
#' 7. If outcome of "Multiple Birth" and "Multiple Birth Unspecified", overwrite "Multiple Birth Unspecified" as "Multiple Birth" for entire pregnancy.
#' 8. If outcome of "Twin Birth" and "Multiple Birth", then classify as "Multiple Birth Unspecified" for entire pregnancy.
#' 9. Entire pregnancy defined as 270 days.
#'
#' @param patient_outcomes_flag A data frame containing the patient_outcomes_flag dataset.
#' @return A data frame with the applied outcome hierarchy.
#' @noRd
apply_outcome_hierarchy_1 <- function(patient_outcomes_flag) {
  # Defining helper functions for hierarchy 1
  
  ## Keep certain outcomes and sort the resulting data frame by `svcdate` in descending order and then by `enrolid`
  keep_outcomes <- function(patient_outcomes_flag, outcomes) {
    patient_outcomes_flag %>%
      filter(outcome %in% outcomes) %>%
      arrange(., desc(svcdate)) %>%
      arrange(enrolid) # need to apply arrange twice to match the sorting order in SAS code, not possible to sort in one step!
  }
  
  ## Drop certain outcomes and and sort the resulting data frame by `svcdate` in descending order and then by `enrolid`
  drop_outcomes <- function(patient_outcomes_flag, outcomes) {
    patient_outcomes_flag %>%
      filter(!(outcome %in% outcomes)) %>%
      arrange(., desc(svcdate)) %>%
      arrange(enrolid) # need to apply arrange twice to match the sorting order in SAS code, not possible to sort in one step!
  }
  
  ## Rename the `outcome_new` column to `outcome` and sort the data frame by `svcdate` in descending order and then by `enrolid`
  rename_and_sort <- function(outcome_multiple) {
    outcome_multiple %>%
      rename(outcome = outcome_new) %>%
      arrange(., desc(svcdate)) %>%
      arrange(enrolid)
  }
  
  # Filter and process the data for different outcome types: "Multiple birth", "Multiple birth*", "Multiple birth unspecified", "Multiple birth unspecified*", "Twin birth", and "Twin birth*".
  outcome_multiple <- keep_outcomes(patient_outcomes_flag, c("Multiple birth", "Multiple birth*"))
  outcome_multiple_unspecified <- keep_outcomes(patient_outcomes_flag, c("Multiple birth unspecified", "Multiple birth unspecified*"))
  outcome_twin <- keep_outcomes(patient_outcomes_flag, c("Twin birth", "Twin birth*"))
  outcome_hierarchy1_dups <- drop_outcomes(patient_outcomes_flag, c("Multiple birth", "Multiple birth*", "Multiple birth unspecified", "Multiple birth unspecified*", "Twin birth", "Twin birth*"))
  
  # Clean the data for each outcome type by calling the `process_outcome_hierarchy_data`, `calculate_outcome_gap_values`, and `modify_outcome_using_hierarchy1_conditions` functions.
  
  ## Clean Multiple birth*
  outcome_multiple_dups <- outcome_multiple %>%
    process_outcome_hierarchy_data("Multiple birth", "mult_") %>%
    calculate_outcome_gap_values("mult_", 6) %>%
    modify_outcome_using_hierarchy1_conditions(
      "Multiple birth*", "Multiple birth",
      "Multiple birth*", "Multiple birth"
    )
  
  ## Clean Multiple birth unspecified*
  outcome_multiple_unsp_dups <- outcome_multiple_unspecified %>%
    process_outcome_hierarchy_data("Multiple birth unspecified", "mult_unsp_") %>%
    calculate_outcome_gap_values("mult_unsp_", 6) %>%
    modify_outcome_using_hierarchy1_conditions(
      "Multiple birth unspecified*", "Multiple birth unspecified",
      "Multiple birth unspecified*", "Multiple birth unspecified"
    )
  
  ## Clean Twin birth*
  outcome_twin_dups <- outcome_twin %>%
    process_outcome_hierarchy_data("Twin birth", "twin_") %>%
    calculate_outcome_gap_values("twin_", 6) %>%
    modify_outcome_using_hierarchy1_conditions(
      "Twin birth*", "Twin birth",
      "Twin birth*", "Twin birth"
    )
  
  # Combine the cleaned data for each outcome type
  outcome_multiple_3_sort <- bind_rows(outcome_twin_dups, outcome_multiple_unsp_dups, outcome_multiple_dups) %>%
    rename_and_sort()
  
  # Apply a series of hierarchy rules to modify the outcomes based on the presence of other outcomes within the same pregnancy.
  
  ## If outcome of Twin Birth and Multiple Birth Unspecified/Multiple Birth Unspecified*, overwrite Multiple Birth Unspecified as Twin Birth for entire pregnancy
  outcome_multiple_6 <- outcome_multiple_3_sort %>%
    process_outcome_hierarchy_data("Twin birth", "dt_") %>%
    calculate_outcome_gap_values("dt_", 4) %>%
    modify_outcome_using_hierarchy1_conditions(
      "Multiple birth unspecified", "Twin birth",
      "Multiple birth unspecified*", "Twin birth"
    )
  
  ## If outcome of Twin Birth* and Multiple Birth Unspecified, overwrite Multiple Birth Unspecified as Twin Birth for entire pregnancy
  ## If outcome of Twin Birth* and Multiple Birth Unspecified*, overwrite Multiple Birth Unspecified as Twin Birth* for entire pregnancy
  outcome_multiple_9 <- outcome_multiple_6 %>%
    rename_and_sort() %>%
    process_outcome_hierarchy_data("Twin birth*", "dt_") %>%
    calculate_outcome_gap_values("dt_", 4) %>%
    modify_outcome_using_hierarchy1_conditions(
      "Multiple birth unspecified", "Twin birth",
      "Multiple birth unspecified*", "Twin birth*"
    )
  
  ## If outcome of Multiple Birth and Multiple Birth Unspecified/Multiple Birth Unspecified*, overwrite Multiple Birth Unspecified as Multiple Birth for entire pregnancy
  outcome_multiple_12 <- outcome_multiple_9 %>%
    rename_and_sort() %>%
    process_outcome_hierarchy_data("Multiple birth", "dt_") %>%
    calculate_outcome_gap_values("dt_", 4) %>%
    modify_outcome_using_hierarchy1_conditions(
      "Multiple birth unspecified", "Multiple birth",
      "Multiple birth unspecified*", "Multiple birth"
    )
  
  ## If outcome of Multiple Birth* and Multiple Birth Unspecified, overwrite Multiple Birth Unspecified as Multiple Birth for entire pregnancy
  ## If outcome of Multiple Birth* and Multiple Birth Unspecified*, overwrite Multiple Birth Unspecified as Multiple Birth* for entire pregnancy
  outcome_multiple_15 <- outcome_multiple_12 %>%
    rename_and_sort() %>%
    process_outcome_hierarchy_data("Multiple birth", "dt_") %>%
    calculate_outcome_gap_values("dt_", 4) %>%
    modify_outcome_using_hierarchy1_conditions(
      "Multiple birth unspecified", "Multiple birth",
      "Multiple birth unspecified*", "Multiple birth*"
    )
  
  ## If outcome of Twin Birth and Multiple Birth/Multiple Birth*, overwrite as Multiple Birth Unspecified for entire pregnancy
  outcome_multiple_18 <- outcome_multiple_15 %>%
    rename_and_sort() %>%
    process_outcome_hierarchy_data("Twin birth", "dt_") %>%
    calculate_outcome_gap_values("dt_", 4) %>%
    modify_outcome_using_hierarchy1_conditions(
      "Multiple birth", "Multiple birth unspecified",
      "Multiple birth*", "Multiple birth unspecified"
    )
  
  ## If outcome of Twin Birth* and Multiple Birth, overwrite as Multiple Birth Unspecified for entire pregnancy
  ## If outcome of Twin Birth* and Multiple Birth*, overwrite as Multiple Birth Unspecified* for entire pregnancy
  outcome_multiple_21 <- outcome_multiple_18 %>%
    rename_and_sort() %>%
    process_outcome_hierarchy_data("Twin birth", "dt_") %>%
    calculate_outcome_gap_values("dt_", 4) %>%
    modify_outcome_using_hierarchy1_conditions(
      "Multiple birth", "Multiple birth unspecified",
      "Multiple birth*", "Multiple birth unspecified*"
    ) %>%
    rename(outcome = outcome_new)
  
  # Combine the final cleaned data with the original data that did not have any of the specified outcomes
  outcome_hierarchy1 <- bind_rows(outcome_hierarchy1_dups, outcome_multiple_21)
  
  }

#' Apply Outcome Hierarchy #2
#'
#' This function applies a second hierarchy to the given outcome_hierarchy1 and labor_delivery_codes datasets.
#' The hierarchy is based on the following rules:
#' 1. Regardless of L&D Codes; if multiple losses occur within 60 days, apply hierarchy
#' 2. Ectopic pregnancy
#' 3. Spontaneous abortion
#' 4. Induced abortion
#' 5. Stillbirth
#' 6. Unclassified pregnancy loss
#'
#' @param outcome_hierarchy1 A data frame containing the outcome_hierarchy1 dataset.
#' @param labor_delivery_codes A data frame containing the labor_delivery_codes dataset.
#' @return A data frame with the applied outcome hierarchy #2.
#' @noRd
apply_outcome_hierarchy_2 <- function(outcome_hierarchy1, labor_delivery_codes) {
  
  # Join L&D Code List to Create Flag
  labor_delivery_flag <- labor_delivery_codes %>%
    mutate(labor_delivery_flag = 1) %>%
    select(description, file, labor_delivery_flag, code)
  
  labor_and_delivery <- outcome_hierarchy1 %>%
    left_join(labor_delivery_flag, by = "code") %>%
    select(-c(file, description.y)) %>%
    rename(description = description.x)
  
  outcome_hierarchy2_losses <- labor_and_delivery %>%
    filter(outcome %in% c("Ectopic pregnancy", "Induced abortion", "Spontaneous abortion", "Stillbirth", "Unclassified pregnancy loss")) %>%
    arrange(., outcome) %>%
    arrange(enrolid)
  
  outcome_hierarchy2_dups <- labor_and_delivery %>%
    filter(!(outcome %in% c("Ectopic pregnancy", "Induced abortion", "Spontaneous abortion", "Stillbirth", "Unclassified pregnancy loss")))
  
  outcome_ectopic3 <- outcome_hierarchy2_losses %>%
    process_outcome_hierarchy_data("Ectopic pregnancy", "dt_") %>%
    calculate_outcome_gap_values("dt_", 4) %>%
    modify_outcome_using_hierarchy2_ectopic_conditions()
  
  outcome_spon3 <- outcome_ectopic3 %>%
    process_outcome_hierarchy_data("Spontaneous abortion", "dt_") %>%
    calculate_outcome_gap_values("dt_", 4) %>%
    modify_outcome_using_hierarchy2_conditions(
      "Ectopic pregnancy", "Spontaneous abortion",
      "Ectopic pregnancy", "Spontaneous abortion",
      "Ectopic pregnancy", "Spontaneous abortion"
    )
  
  outcome_induced3 <- outcome_spon3 %>%
    process_outcome_hierarchy_data("Induced abortion", "dt_") %>%
    calculate_outcome_gap_values("dt_", 4) %>%
    modify_outcome_using_hierarchy2_induced_conditions()
  
  outcome_still3 <- outcome_induced3 %>%
    process_outcome_hierarchy_data("Stillbirth", "dt_") %>%
    calculate_outcome_gap_values("dt_", 4) %>%
    modify_outcome_using_hierarchy2_stillbirth_conditions()
  
  outcome_hierarchy2 <- bind_rows(outcome_hierarchy2_dups, outcome_still3)
  
  }

#' Apply Outcome Hierarchy #3
#'
#' This function applies the third outcome hierarchy to the given outcome_hierarchy2 dataset.
#' The hierarchy is based on the following rules:
#' 1. No multiples (within 180 days)
#' 2. Loss outcome within -30 days to +60 days of a Labor & Delivery Code
#' 3. Labor & Delivery outcome will be overwritten
#' 4. Spontaneous abortion & Labor & Delivery Codes = Spontaneous abortion
#' 5. Ectopic & Labor & Delivery Codes = Ectopic
#' 6. Induced abortion & Labor & Delivery Codes = Induced abortion
#' 7. Stillbirth abortion & Labor & Delivery Codes = Stillbirth
#' 8. Unclassified outcomes >= Labor & Delivery Codes = Unclassified
#' 9. Unclassified < Labor & Delivery Codes = Pregnancy*
#'
#' @param outcome_hierarchy2 A data frame containing the outcome_hierarchy2 dataset.
#' @importFrom stringr str_c
#' @return A list containing three data frames: outcome_hierarchy3, outcome_hierarchy3_nondups, and multiple_lt_180_days.
#' @noRd
apply_outcome_hierarchy_3 <- function(outcome_hierarchy2) {
  
  # Note: To split data, it is more efficient to group_split by column and save an intermediate object instead of filtering twice
  split_data <- outcome_hierarchy2 %>%
    mutate(data_split_flag = case_when(
      (multiple_flag == 1 | loss_flag == 1 | labor_delivery_flag == 1) ~ 1,
      TRUE ~ 2
    ))
  
  # Create a data frame for non-duplicates
  outcome_hierarchy3_nondups <- split_data %>%
    filter(data_split_flag == 2)
  
  # Create a data frame for duplicates
  outcome_hierarchy3_dups <- split_data %>%
    filter(data_split_flag == 1) %>%
    arrange(., desc(svcdate)) %>%
    arrange(enrolid, outcome)
  
  # Process multiple_flag data
  multiple <- outcome_hierarchy3_dups %>%
    filter(multiple_flag == 1) %>%
    select(c(enrolid, svcdate)) %>%
    group_by(enrolid) %>%
    mutate(row = row_number()) %>%
    pivot_wider(
      names_from = row,
      values_from = svcdate
    ) %>%
    rename(enrolid_copy = enrolid) %>%
    rename_with(function(x) stringr::str_c("dt_", x), -c(enrolid_copy))
  
  # Join multiple data with duplicates
  multiple2 <- outcome_hierarchy3_dups %>%
    left_join(multiple, by = c("enrolid" = "enrolid_copy"), keep = TRUE) %>%
    calculate_outcome_gap_values("dt_", 4) %>%
    modify_outcome_using_hierarchy3_multiple_conditions()
  
  # Separate data based on 180 days condition
  multiple_gt_180_days <- multiple2 %>%
    filter(flag == "gt_180") %>%
    arrange(., (svcdate)) %>%
    arrange(enrolid, outcome)
  
  multiple_lt_180_days <- multiple2 %>%
    filter(flag == "lt_180") %>%
    arrange(., (svcdate)) %>%
    arrange(enrolid, outcome)
  
  # Apply a series of hierarchy rules to modify the outcomes based on the presence of other outcomes within the same pregnancy.
  
  outcome_ectopic3 <- multiple_gt_180_days %>%
    process_outcome_hierarchy_data("Ectopic pregnancy", "dt_", group_by_enrolid_only = TRUE) %>%
    modify_outcome_using_hierarchy3_ectopic_conditions()
  
  outcome_spon3 <- outcome_ectopic3 %>%
    process_outcome_hierarchy_data("Spontaneous abortion", "dt_") %>%
    modify_outcome_using_hierarchy3_spontaneous_conditions()
  
  outcome_induced3 <- outcome_spon3 %>%
    process_outcome_hierarchy_data("Induced abortion", "dt_") %>%
    modify_outcome_using_hierarchy3_induced_conditions()
  
  outcome_still3 <- outcome_induced3 %>%
    process_outcome_hierarchy_data("Stillbirth", "dt_") %>%
    modify_outcome_using_hierarchy3_stillbirth_conditions()
  
  outcome_unclass_loss2_b <- outcome_still3 %>%
    process_outcome_hierarchy_data("Unclassified pregnancy loss", "dt_") %>%
    modify_outcome_using_hierarchy3_unclassified_loss_conditions()
  
  other <- outcome_unclass_loss2_b %>%
    filter(data_flag == 2)
  
  loss_l_and_d <- outcome_unclass_loss2_b %>%
    filter(data_flag == 1) %>%
    arrange(., (svcdate)) %>%
    arrange(enrolid, outcome)
  
  loss_l_and_d2 <- loss_l_and_d %>%
    filter(labor_delivery_flag == 1) %>%
    group_by(enrolid, dt_1, dt_2, dt_3, dt_4, dt_5, dt_6, dt_7, dt_8, dt_9, dt_10) %>%
    summarise(
      svcdate = n(),
      cnt_loss_max = max(cnt_loss)
    ) %>%
    rename(cnt_l_and_d = svcdate) %>%
    select(enrolid, dt_1, dt_2, dt_3, dt_4, dt_5, dt_6, dt_7, dt_8, dt_9, dt_10, cnt_l_and_d, cnt_loss_max)
  
  loss_l_and_d3 <- loss_l_and_d %>%
    left_join(loss_l_and_d2, by = c("enrolid", "dt_1", "dt_2", "dt_3", "dt_4", "dt_5", "dt_6", "dt_7", "dt_8", "dt_9", "dt_10"))
  
  loss_l_and_d4 <- loss_l_and_d3 %>%
    mutate(
      outcome = case_when(
        !is.na(cnt_l_and_d) & !is.na(cnt_loss_max) & cnt_loss_max >= cnt_l_and_d &
          (labor_delivery_flag == 1 & !(outcome %in% c("Ectopic pregnancy", "Spontaneous abortion", "Induced abortion", "Stillbirth"))) ~ "Unclassified pregnancy loss",
        !is.na(cnt_l_and_d) & !is.na(cnt_loss_max) & cnt_loss_max < cnt_l_and_d & (outcome %in% c("Unclassified pregnancy loss")) ~ "Pregnancy*",
        TRUE ~ outcome
      )
    ) %>%
    select(
      enrolid, svcdate, code, claims_file, description, final_trimester_code, srv_year, outcome, pdx,
      trimester_code_clean, newborn_flag, multiple_flag, labor_delivery_flag, cnt_loss, cnt_l_and_d
    )
  
  outcome_hierarchy3 <- bind_rows(other, loss_l_and_d4) %>%
    select(
      enrolid, svcdate, code, claims_file, description, final_trimester_code, srv_year, outcome,
      pdx, trimester_code_clean, newborn_flag, multiple_flag, labor_delivery_flag
    )
  
  # Return the final data frames
  return(list(
    outcome_hierarchy3 = outcome_hierarchy3,
    outcome_hierarchy3_nondups = outcome_hierarchy3_nondups,
    multiple_lt_180_days = multiple_lt_180_days
  ))
}

#' Apply Outcome Hierarchy #4
#'
#' This function applies the fourth hierarchy to the given outcome_hierarchy3, outcome_hierarchy3_nondups, and multiple_lt_180_days datasets.
#' The hierarchy is based on the following rules:
#' 1. No multiple pregnancy within 180 days
#' 2. Labor & Delivery Codes within +60 days to +180 days of a Loss outcome
#' 3. Loss outcome will be overwritten
#' 4. Ectopic & more than 1 Labor & Delivery Codes = Pregnancy*
#' 5. Spontaneous abortion & more than 1 Labor & Delivery Codes = Pregnancy*
#' 6. Induced abortion & more than 1 Labor & Delivery Codes = Pregnancy*
#' 7. Stillbirth abortion & more than 1 Labor & Delivery Codes = Pregnancy*
#' 8. Unclassified & more than 1 Labor & Delivery Codes = Pregnancy*
#'
#' @param outcome_hierarchy3 A data frame containing the outcome_hierarchy3 dataset.
#' @param outcome_hierarchy3_nondups A data frame containing the outcome_hierarchy3_nondups dataset.
#' @param multiple_lt_180_days A data frame containing the multiple_lt_180_days dataset.
#' @importFrom stringr str_c
#' @return A data frame with the applied outcome hierarchy.
#' @noRd
apply_outcome_hierarchy_4 <- function(outcome_hierarchy3, outcome_hierarchy3_nondups, multiple_lt_180_days) {
  
  # Arrange input dataset by enrolid and svcdate
  outcome_hierarchy4_dups <- outcome_hierarchy3 %>%
    arrange(enrolid, svcdate)
  
  # Filter and select relevant columns for Labor & Delivery outcomes
  outcome_l_and_d <- outcome_hierarchy4_dups %>%
    filter(labor_delivery_flag == 1 & !(outcome %in% c("Ectopic pregnancy", "Spontaneous abortion", "Induced abortion", "Stillbirth", "Unclassified pregnancy loss"))) %>%
    select(c(enrolid, svcdate)) %>%
    group_by(enrolid) %>%
    mutate(row = row_number()) %>%
    pivot_wider(
      names_from = row,
      values_from = svcdate
    ) %>%
    rename(enrolid_copy = enrolid) %>%
    rename_with(function(x) stringr::str_c("dt_", x), -c(enrolid_copy))
  
  # Enrich input data set and apply hierarchy 4
  outcome_l_and_d_final <- outcome_hierarchy4_dups %>%
    left_join(outcome_l_and_d, by = c("enrolid" = "enrolid_copy"), keep = TRUE) %>%
    modify_outcome_using_hierarchy4_conditions()
  
  # Combine the outcome_hierarchy3_nondups, multiple_lt_180_days, and outcome_l_and_d_final datasets
  bind_rows(outcome_hierarchy3_nondups, multiple_lt_180_days, outcome_l_and_d_final) %>%
    select(
      enrolid, svcdate, code, claims_file, description, final_trimester_code, srv_year, outcome,
      pdx, trimester_code_clean, newborn_flag, multiple_flag, loss_flag, labor_delivery_flag
    )
}

#' Apply Outcome Hierarchy #5
#'
#' This function applies the fifth hierarchy to the given outcome_hierarchy4 dataset.
#' The hierarchy is based on the following rules:
#' If multiple Term outcomes occur within -60 days to +60 days of each other, apply the following hierarchy:
#' a. Pre-term
#' b. Term
#' c. Post-term
#' d. Post-term*
#'
#' @param outcome_hierarchy4 A data frame containing the outcome_hierarchy4 dataset.
#' @return A data frame with the applied outcome hierarchy.
#' @noRd
apply_outcome_hierarchy_5 <- function(outcome_hierarchy4) {
  
  outcome_hierarchy5_dups <- outcome_hierarchy4 %>%
    filter(outcome %in% c("Post-term birth", "Preterm birth", "Term birth", "Post-term birth*")) %>%
    arrange(enrolid, svcdate)
  
  outcome_hierarchy5_nodups <- outcome_hierarchy4 %>%
    filter(!(outcome %in% c("Post-term birth", "Preterm birth", "Term birth", "Post-term birth*")))
  
  preterm <- outcome_hierarchy5_dups %>%
    process_outcome_hierarchy_data("Preterm birth", "dt_", group_by_enrolid_only = TRUE) %>%
    calculate_outcome_gap_values("dt_", 4) %>%
    modify_outcome_using_hierarchy5_preterm_conditions()
  
  term <- preterm %>%
    process_outcome_hierarchy_data("Term birth", "dt_", group_by_enrolid_only = TRUE) %>%
    calculate_outcome_gap_values("dt_", 4) %>%
    modify_outcome_using_hierarchy5_term_conditions()
  
  postterm <- term %>%
    process_outcome_hierarchy_data("Post-term birth", "dt_", group_by_enrolid_only = TRUE) %>%
    calculate_outcome_gap_values("dt_", 4) %>%
    modify_outcome_using_hierarchy5_postterm_conditions()
  
  outcome_hierarchy5 <- bind_rows(outcome_hierarchy5_nodups, postterm)
}

#' Process Outcome Hierarchy Data
#'
#' This function processes and modifies outcome hierarchy data by transposing and joining. It takes a data frame as input and returns a modified data frame with updated outcome hierarchy data.
#'
#' @param data A data frame containing the input data.
#' @param outcome_filter A filter value for the 'outcome' column.
#' @param col_start_pattern A pattern for column names containing attributes.
#' @param group_by_enrolid_only lgl, data are grouped by enrolid before pivoting if TRUE, defaults to FALSE.
#' @return A data frame with modified outcome hierarchy data.
#' @noRd
process_outcome_hierarchy_data <- function(data, outcome_filter, col_start_pattern, group_by_enrolid_only = FALSE) {
  # Transpose the input data frame based on the specified outcome filter and column name pattern
  transposed_df <- data %>%
    select(enrolid, outcome, svcdate) %>%
    filter(outcome == outcome_filter)
  
  if (group_by_enrolid_only == FALSE) {
    transposed_df <- transposed_df %>% group_by(enrolid, outcome)
  } else {
    transposed_df <- transposed_df %>% group_by(enrolid)
  }
  
  transposed_df <- transposed_df %>%
    mutate(row = row_number()) %>%
    pivot_wider(
      names_from = row,
      values_from = svcdate
    ) %>%
    rename(enrolid_copy = enrolid) %>%
    rename_with(function(x) stringr::str_c(col_start_pattern, x), -c(enrolid_copy, outcome)) %>%
    ungroup() %>%
    select(-outcome)
  
  # Join the transposed data frame with the original data frame
  joined_df <- data %>%
    left_join(transposed_df, by = c("enrolid" = "enrolid_copy"), keep = TRUE)
  
}

#' Process and Modify Outcome Hierarchy Data
#'
#' This function processes and modifies outcome hierarchy data by transposing, joining, and calculating gap values. It takes a data frame as input and returns a modified data frame with updated outcome hierarchy data.
#'
#' @param joined_df A data frame containing the input data.
#' @param col_start_pattern A pattern for column names containing attributes.
#' @param substr_start_pos The starting position for extracting gap column names.
#' @return A data frame with modified outcome hierarchy data.
#' @noRd
calculate_outcome_gap_values <- function(joined_df, col_start_pattern, substr_start_pos) {
  # Create a list of col_start_pattern columns to iterate through in the for loop
  selected_cols <- grep(paste0("^", col_start_pattern), colnames(joined_df), value = TRUE)
  
  # Calculate gap values for each col_start_pattern column and add them to the joined data frame
  for (col_name in selected_cols) {
    gap_value <- joined_df$svcdate - joined_df[[col_name]]
    gap_col_name <- paste0("gap", trimws(substr(col_name, start = substr_start_pos, stop = nchar(col_name))))
    joined_df[gap_col_name] <- abs(as.numeric(gap_value)) # assigning calculated gap value
  }
  
  joined_df
}

#' Modify Outcome Hierarchy 1 Data
#'
#' This function modifies outcome hierarchy data by calculating gap values. It takes a data frame as input and returns a modified data frame with updated outcome hierarchy data.
#'
#' @param data A data frame containing the input data.
#' @param outcome_case_cond_1 The first condition for the 'outcome' column.
#' @param outcome_case_out_1 The output value for the first condition of the 'outcome' column.
#' @param outcome_case_cond_2 The second condition for the 'outcome' column.
#' @param outcome_case_out_2 The output value for the second condition of the 'outcome' column.
#'
#' @return A data frame with modified outcome hierarchy data.
#' @noRd
modify_outcome_using_hierarchy1_conditions <- function(data, outcome_case_cond_1, outcome_case_out_1, outcome_case_cond_2, outcome_case_out_2) {
  # Modify the outcome column based on the specified conditions and gap values.
  data %>%
    mutate(
      min_gap = suppressWarnings(apply(select(., starts_with("gap")), MARGIN = 1, FUN = min, na.rm = TRUE)),
      outcome_new = case_when(
        outcome %in% c(outcome_case_cond_1) & (0 <= min_gap) & (min_gap <= 270) ~ outcome_case_out_1,
        outcome %in% c(outcome_case_cond_2) & (0 <= min_gap) & (min_gap <= 270) ~ outcome_case_out_2,
        TRUE ~ outcome
      )
    ) %>%
    select(enrolid, svcdate, code, claims_file, description, final_trimester_code, srv_year, outcome_new, pdx, trimester_code_clean, newborn_flag, multiple_flag, loss_flag)
}
#' Modify Outcome Hierarchy 2 Data
#'
#' This function modifies outcome hierarchy data by calculating gap values. It takes a data frame as input and returns a modified data frame with updated outcome hierarchy data.
#'
#' @param data A data frame containing the input data.
#' @param outcome_case_cond_1 The first condition for the 'outcome' column.
#' @param outcome_case_out_1 The output value for the first condition of the 'outcome' column.
#' @param outcome_case_cond_2 The second condition for the 'outcome' column.
#' @param outcome_case_out_2 The output value for the second condition of the 'outcome' column.
#' @param outcome_case_cond_3 The third condition for the 'outcome' column (optional).
#' @param outcome_case_out_3 The output value for the third condition of the 'outcome' column (optional).
#' @param not_in TRUE/FALSE, changes the behavior of the function (optional, default is FALSE).
#'
#' @return A data frame with modified outcome hierarchy data.
#' @noRd
modify_outcome_using_hierarchy2_conditions <- function(data, outcome_case_cond_1, outcome_case_out_1, outcome_case_cond_2, outcome_case_out_2, outcome_case_cond_3 = NULL, outcome_case_out_3 = NULL, not_in = FALSE) {
  # Modify the outcome column based on the specified conditions and gap values.
  data %>%
    mutate(
      min_gap = suppressWarnings(apply(select(., starts_with("gap")), MARGIN = 1, FUN = min, na.rm = TRUE)),
      min_gap = if_else(is.infinite(min_gap), NA_real_, min_gap),
      outcome = case_when(
        !(outcome %in% c(outcome_case_cond_1)) & (0 <= min_gap) & (min_gap <= 60) ~ outcome_case_out_1,
        !(outcome %in% c(outcome_case_cond_2)) & (0 <= min_gap) & (min_gap <= 60) ~ outcome_case_out_2,
        !(outcome %in% c(outcome_case_cond_3)) & (0 <= min_gap) & (min_gap <= 60) ~ outcome_case_out_3,
        TRUE ~ outcome
      )
    ) %>%
    select(enrolid, svcdate, code, claims_file, description, final_trimester_code, srv_year, outcome, pdx, trimester_code_clean, newborn_flag, multiple_flag, loss_flag, labor_delivery_flag, min_gap)
}

#' Modify Outcome Using Hierarchy 2 Ectopic Conditions
#'
#' This function modifies the outcome data by updating the 'outcome' column based on ectopic pregnancy conditions. It takes a data frame as input and returns a modified data frame with updated outcome data.
#'
#' @param data A data frame containing the input data.
#'
#' @return A data frame with modified outcome data.
#'
#' @noRd
modify_outcome_using_hierarchy2_ectopic_conditions <- function(data) {
  # need to replace inf values with NA in min_gap
  data %>%
    mutate(
      min_gap = suppressWarnings(apply(select(., starts_with("gap")), MARGIN = 1, FUN = min, na.rm = TRUE)),
      min_gap = if_else(is.infinite(min_gap), NA_real_, min_gap),
      outcome = case_when(
        (0 <= min_gap & min_gap <= 60) ~ "Ectopic pregnancy",
        TRUE ~ outcome
      )
    ) %>%
    select(
      enrolid, svcdate, code, claims_file, description, final_trimester_code, srv_year, outcome, min_gap, pdx,
      trimester_code_clean, newborn_flag, multiple_flag, loss_flag, labor_delivery_flag
    )
}

#' Modify Outcome Using Hierarchy 2 Induced Conditions
#'
#' This function modifies the outcome data by updating the 'outcome' column based on induced abortion conditions. It takes a data frame as input and returns a modified data frame with updated outcome data.
#'
#' @param data A data frame containing the input data.
#'
#' @return A data frame with modified outcome data.
#'
#' @noRd
modify_outcome_using_hierarchy2_induced_conditions <- function(data) {
  data %>%
    mutate(
      min_gap = suppressWarnings(apply(select(., starts_with("gap")), MARGIN = 1, FUN = min, na.rm = TRUE)),
      min_gap = if_else(is.infinite(min_gap), NA_real_, min_gap),
      outcome = case_when(
        !(outcome %in% c("Ectopic pregnancy", "Spontaneous abortion")) & (0 <= min_gap) & (min_gap <= 60) ~ "Induced abortion",
        TRUE ~ outcome
      )
    ) %>%
    select(enrolid, svcdate, code, claims_file, description, final_trimester_code, srv_year, outcome, pdx, trimester_code_clean, newborn_flag, multiple_flag, loss_flag, labor_delivery_flag, min_gap)
}

#' Modify Outcome Using Hierarchy 2 Stillbirth Conditions
#'
#' This function modifies the outcome data by updating the 'outcome' column based on stillbirth conditions. It takes a data frame as input and returns a modified data frame with updated outcome data.
#'
#' @param data A data frame containing the input data.
#'
#' @return A data frame with modified outcome data.
#'
#' @noRd
modify_outcome_using_hierarchy2_stillbirth_conditions <- function(data) {
  data %>%
    mutate(
      min_gap = suppressWarnings(apply(select(., starts_with("gap")), MARGIN = 1, FUN = min, na.rm = TRUE)),
      min_gap = if_else(is.infinite(min_gap), NA_real_, min_gap),
      outcome = case_when(
        !(outcome %in% c("Ectopic pregnancy", "Spontaneous abortion", "Induced abortion")) & (0 <= min_gap) & (min_gap <= 60) ~ "Stillbirth",
        TRUE ~ outcome
      )
    ) %>%
    select(enrolid, svcdate, code, claims_file, description, final_trimester_code, srv_year, outcome, pdx, trimester_code_clean, newborn_flag, multiple_flag, loss_flag, labor_delivery_flag)
}

#' Modify outcome using hierarchy3 multiple conditions
#'
#' This function modifies the outcome of the input data frame based on the hierarchy3 multiple conditions.
#'
#' @param multiple A data frame containing the multiple dataset.
#' @return A data frame with modified outcomes based on hierarchy3 multiple conditions.
#' @noRd
modify_outcome_using_hierarchy3_multiple_conditions <- function(multiple) {
  multiple %>%
    mutate(
      min_gap = suppressWarnings(apply(select(., starts_with("gap")), MARGIN = 1, FUN = min, na.rm = TRUE)),
      min_gap = if_else(is.infinite(min_gap), NA_real_, min_gap),
      flag = case_when(
        (0 <= min_gap & min_gap <= 180) ~ "lt_180",
        (is.na(min_gap) | min_gap > 180) ~ "gt_180"
      )
    ) %>%
    select(
      enrolid, svcdate, code, claims_file, description, final_trimester_code, srv_year, outcome, flag, pdx,
      trimester_code_clean, min_gap, newborn_flag, multiple_flag, loss_flag, labor_delivery_flag
    )
}

#' Modify outcome using hierarchy3 ectopic conditions
#'
#' This function modifies the outcome of the input data frame based on the hierarchy3 ectopic conditions.
#'
#' @param joined_df A data frame containing the joined dataset.
#' @return A data frame with modified outcomes based on hierarchy3 ectopic conditions.
#' @noRd
modify_outcome_using_hierarchy3_ectopic_conditions <- function(joined_df) {
  # create a list of dt_* columns that we need to iterate through the for loop
  selected_cols <- grep("^dt_", colnames(joined_df), value = TRUE)
  
  # run 'for loop' for the number of times we have dt_ columns to add gap columns
  for (col_name in selected_cols)
  {
    gap_value <- joined_df$svcdate - joined_df[[col_name]]
    gap_col_name <- paste0("gap", trimws(substr(col_name, start = 4, stop = nchar(col_name))))
    joined_df[gap_col_name] <- abs(as.numeric(gap_value)) # assigning calculated gap value
    
    flag_1_value <- case_when((gap_value >= -30 & gap_value <= 60) ~ 1, TRUE ~ 0)
    flag_1_col_name <- paste0("temp_flag_1_", trimws(substr(col_name, start = 4, stop = nchar(col_name))))
    joined_df[flag_1_col_name] <- (as.numeric(flag_1_value)) # assigning flag_2 value to flag_2_1, flag_2_2 .. columns
  }
  
  joined_df %>%
    mutate(
      flag_1 = suppressWarnings(apply(select(., starts_with("temp_flag_1_")), MARGIN = 1, FUN = max, na.rm = TRUE)),
      outcome = case_when(
        labor_delivery_flag == 1 & flag_1 == 1 ~ "Ectopic pregnancy",
        TRUE ~ outcome
      )
    ) %>%
    select(
      enrolid, svcdate, code, claims_file, description, final_trimester_code, srv_year, outcome, pdx,
      trimester_code_clean, newborn_flag, multiple_flag, loss_flag, labor_delivery_flag, flag_1
    )
}

#' Modify outcome using hierarchy3 spontaneous conditions
#'
#' This function modifies the outcome of the input data frame based on the hierarchy3 spontaneous conditions.
#'
#' @param joined_df A data frame containing the joined dataset.
#' @return A data frame with modified outcomes based on hierarchy3 spontaneous conditions.
#' @noRd
modify_outcome_using_hierarchy3_spontaneous_conditions <- function(joined_df) {
  # create a list of dt_* columns that we need to iterate through the for loop
  selected_cols <- grep("^dt_", colnames(joined_df), value = TRUE)
  
  # run 'for loop' for the number of times we have dt_ columns to add gap columns
  for (col_name in selected_cols)
  {
    gap_value <- as.numeric(joined_df$svcdate - joined_df[[col_name]])
    gap_col_name <- paste0("gap", trimws(substr(col_name, start = 4, stop = nchar(col_name))))
    joined_df[gap_col_name] <- gap_value # assigning calculated gap value
    flag_2_value <- case_when((gap_value >= -30 & gap_value <= 60) ~ 1, TRUE ~ 0)
    flag_2_col_name <- paste0("temp_flag_2_", trimws(substr(col_name, start = 4, stop = nchar(col_name))))
    joined_df[flag_2_col_name] <- (as.numeric(flag_2_value)) # assigning flag_2 value to flag_2_1, flag_2_2 .. columns
  }
  
  joined_df %>%
    mutate(
      flag_2 = suppressWarnings(apply(select(., starts_with("temp_flag_2_")), MARGIN = 1, FUN = max, na.rm = TRUE)),
      outcome = case_when(
        labor_delivery_flag == 1 & outcome != "Ectopic pregnancy" & flag_2 == 1 ~ "Spontaneous abortion",
        TRUE ~ outcome
      )
    ) %>%
    select(
      enrolid, svcdate, code, claims_file, description, final_trimester_code, srv_year, outcome, flag_2, pdx,
      trimester_code_clean, newborn_flag, multiple_flag, loss_flag, labor_delivery_flag
    )
}


#' Modify outcome using hierarchy3 induced conditions
#'
#' This function modifies the outcome of the input data frame based on the hierarchy3 induced conditions.
#'
#' @param joined_df A data frame containing the joined dataset.
#' @return A data frame with modified outcomes based on hierarchy3 induced conditions.
#' @noRd
modify_outcome_using_hierarchy3_induced_conditions <- function(joined_df) {
  # create a list of dt_* columns that we need to iterate through the for loop
  selected_cols <- grep("^dt_", colnames(joined_df), value = TRUE)
  
  # run 'for loop' for the number of times we have dt_ columns to add gap columns
  for (col_name in selected_cols) {
    gap_value <- joined_df$svcdate - joined_df[[col_name]]
    gap_col_name <- paste0("gap", trimws(substr(col_name, start = 4, stop = nchar(col_name))))
    joined_df[gap_col_name] <- gap_value # assigning calculated gap value
    flag_3_value <- case_when((gap_value >= -30 & gap_value <= 60) ~ 1, TRUE ~ 0) # defaulting to zero, otherwise max can't be performed over NA!
    flag_3_col_name <- paste0("temp_flag_3_", trimws(substr(col_name, start = 4, stop = nchar(col_name))))
    joined_df[flag_3_col_name] <- (as.numeric(flag_3_value)) # assigning flag_3 value to flag_3_1, flag_3_2 .. columns
  }
  
  joined_df %>%
    mutate(
      flag_3 = suppressWarnings(apply(select(., starts_with("temp_flag_3_")), MARGIN = 1, FUN = max, na.rm = TRUE)),
      outcome = case_when(
        (labor_delivery_flag == 1) & !(outcome %in% c("Ectopic pregnancy", "Spontaneous abortion")) & (flag_3 == 1) ~ "Induced abortion",
        TRUE ~ outcome
      )
    ) %>%
    select(
      enrolid, svcdate, code, claims_file, description, final_trimester_code, srv_year, outcome, flag_3, pdx,
      trimester_code_clean, newborn_flag, multiple_flag, loss_flag, labor_delivery_flag
    )
}

#' Modify outcome using hierarchy3 stillbirth conditions
#'
#' This function modifies the outcome of the input data frame based on the hierarchy3 stillbirth conditions.
#'
#' @param joined_df A data frame containing the joined dataset.
#' @return A data frame with modified outcomes based on hierarchy3 stillbirth conditions.
#' @noRd
modify_outcome_using_hierarchy3_stillbirth_conditions <- function(joined_df) {
  # create a list of dt_* columns that we need to iterate through the for loop
  selected_cols <- grep("^dt_", colnames(joined_df), value = TRUE)
  
  # run 'for loop' for the number of times we have dt_ columns to add gap columns
  for (col_name in selected_cols)
  {
    gap_value <- joined_df$svcdate - joined_df[[col_name]]
    gap_col_name <- paste0("gap", trimws(substr(col_name, start = 4, stop = nchar(col_name))))
    joined_df[gap_col_name] <- gap_value # assigning calculated gap value
    flag_4_value <- case_when((gap_value >= -30 & gap_value <= 60) ~ 1, TRUE ~ 0)
    flag_4_col_name <- paste0("temp_flag_4_", trimws(substr(col_name, start = 4, stop = nchar(col_name))))
    joined_df[flag_4_col_name] <- (as.numeric(flag_4_value))
  }
  
  joined_df %>%
    mutate(
      flag_4 = suppressWarnings(apply(select(., starts_with("temp_flag_4_")), MARGIN = 1, FUN = max, na.rm = TRUE)),
      outcome = case_when(
        labor_delivery_flag == 1 & (!(outcome %in% c("Ectopic pregnancy", "Spontaneous abortion", "Induced abortion"))) & flag_4 == 1 ~ "Stillbirth",
        TRUE ~ outcome
      )
    ) %>%
    select(
      enrolid, svcdate, code, claims_file, description, final_trimester_code, srv_year, outcome, flag_4, pdx,
      trimester_code_clean, newborn_flag, multiple_flag, loss_flag, labor_delivery_flag
    )
}

#' Modify outcome using hierarchy3 unclassified loss conditions
#'
#' This function modifies the outcome of the input data frame based on the hierarchy3 unclassified loss conditions.
#'
#' @param joined_df A data frame containing the joined dataset.
#' @return A data frame with modified outcomes based on hierarchy3 unclassified loss conditions.
#' @noRd
modify_outcome_using_hierarchy3_unclassified_loss_conditions <- function(joined_df) {
  joined_df <- joined_df %>% rename(los_flag = loss_flag)
  
  # create a list of dt_* columns that we need to iterate through the for loop
  selected_cols <- grep("^dt_", colnames(joined_df), value = TRUE)
  
  # run 'for loop' for the number of times we have dt_ columns to add gap columns
  for (col_name in selected_cols)
  {
    gap_value <- as.numeric(joined_df$svcdate - joined_df[[col_name]]) # calculating the gap_value for the first time in the loop
    gap_value <- case_when((is.na(gap_value) | gap_value < -30 | gap_value > 60) ~ NA_real_, TRUE ~ gap_value) # updating the gap_value when it meets a certain condition
    gap_col_name <- paste0("gap", trimws(substr(col_name, start = 4, stop = nchar(col_name)))) # creating dynamic column names e.g. gap1, gap2
    joined_df[gap_col_name] <- (as.numeric(gap_value)) # assigning calculated gap value
    
    loss_value <- case_when(!(is.na(gap_value)) ~ 1)
    loss_col_name <- paste0("loss", trimws(substr(col_name, start = 4, stop = nchar(col_name))))
    joined_df[loss_col_name] <- (as.numeric(loss_value)) # assigning calculated gap value
    
    flag_5_value <- case_when((gap_value >= -30 & gap_value <= 60) ~ 1, TRUE ~ 0)
    flag_5_col_name <- paste0("temp_flag_5_", trimws(substr(col_name, start = 4, stop = nchar(col_name))))
    joined_df[flag_5_col_name] <- (as.numeric(flag_5_value)) # assigning flag_5 value to flag_5_1, flag_5_2 .. columns
  }
  
  joined_df %>%
    mutate(
      cnt_loss = suppressWarnings(apply(select(., starts_with("loss")), MARGIN = 1, FUN = sum, na.rm = TRUE)),
      flag_5 = suppressWarnings(apply(select(., starts_with("temp_flag_5_")), MARGIN = 1, FUN = max, na.rm = TRUE)),
      data_flag = case_when(
        (outcome == "Unclassified pregnancy loss") |
          (labor_delivery_flag == 1 & (!(outcome %in% c("Ectopic pregnancy", "Spontaneous abortion", "Induced abortion", "Stillbirth")))) &
          flag_5 == 1 ~ 1,
        TRUE ~ 2
      )
    ) %>%
    rename(loss_flag = los_flag) %>%
    select(-(starts_with(c("gap", "temp_flag_5_"))))
}

#' Modify Outcome Using Hierarchy4 Conditions
#'
#' This function modifies the outcome of the given data based on the conditions
#' in the hierarchy4.
#'
#' @param data A data frame containing the input data.
#' @return A data frame with the modified outcome based on the hierarchy4 conditions.
#' @noRd
modify_outcome_using_hierarchy4_conditions <- function(data) {
  
  # create a list of dt_* columns that we need to iterate through the for loop
  selected_cols <- grep("^dt_", colnames(data), value = TRUE)
  
  # run 'for loop' for the number of times we have dt_ columns to add gap columns
  for (col_name in selected_cols)
  {
    gap_value <- as.numeric(data[[col_name]] - data$svcdate) # calculating the gap_value for the first time in the loop
    gap_col_name <- paste0("gap", trimws(substr(col_name, start = 4, stop = nchar(col_name)))) # creating dynamic column names e.g. gap1, gap2
    data[gap_col_name] <- gap_value # assigning calculated gap value
    
    lnd_value <- case_when((gap_value > 60 & gap_value <= 180) ~ 1, TRUE ~ 0)
    lnd_col_name <- paste0("lnd", trimws(substr(col_name, start = 4, stop = nchar(col_name))))
    data[lnd_col_name] <- (as.numeric(lnd_value)) # assigning flag_5 value to flag_5_1, flag_5_2 .. columns
  }
  
  outcome_l_and_d3 <- data %>%
    mutate(
      cnt_lnd = suppressWarnings(apply(select(., starts_with("lnd")), MARGIN = 1, FUN = sum, na.rm = TRUE)),
      outcome = case_when(
        (outcome %in% c("Ectopic pregnancy", "Spontaneous abortion", "Induced abortion", "Stillbirth", "Unclassified pregnancy loss")) & cnt_lnd > 1 ~ "Pregnancy*",
        TRUE ~ outcome
      )
    )
  
  return(outcome_l_and_d3)
}

#' Modify Outcome Using Hierarchy5 Preterm Conditions
#'
#' This function modifies the outcome of the given data based on the preterm conditions
#' in the hierarchy5.
#'
#' @param data A data frame containing the input data.
#' @return A data frame with the modified outcome based on the preterm conditions.
#' @noRd
modify_outcome_using_hierarchy5_preterm_conditions <- function(data) {
  data %>%
    mutate(
      min_gap = suppressWarnings(apply(select(., starts_with("gap")), MARGIN = 1, FUN = min, na.rm = TRUE)),
      min_gap = if_else(is.infinite(min_gap), NA_real_, min_gap),
      outcome = case_when(
        min_gap >= 0 & min_gap <= 60 ~ "Preterm birth",
        TRUE ~ outcome
      )
    ) %>%
    select(
      enrolid, svcdate, code, claims_file, description, final_trimester_code, srv_year, outcome,
      pdx, trimester_code_clean, newborn_flag, multiple_flag, loss_flag, labor_delivery_flag
    )
}

#' Modify Outcome Using Hierarchy5 Term Conditions
#'
#' This function modifies the outcome of the given data based on the term conditions
#' in the hierarchy5.
#'
#' @param data A data frame containing the input data.
#' @return A data frame with the modified outcome based on the term conditions.
#' @noRd
modify_outcome_using_hierarchy5_term_conditions <- function(data) {
  data %>%
    mutate(
      min_gap = suppressWarnings(apply(select(., starts_with("gap")), MARGIN = 1, FUN = min, na.rm = TRUE)),
      min_gap = if_else(is.infinite(min_gap), NA_real_, min_gap),
      outcome = case_when(
        !(outcome %in% ("Preterm birth")) & min_gap >= 0 & min_gap <= 60 ~ "Term birth",
        TRUE ~ outcome
      )
    ) %>%
    select(
      enrolid, svcdate, code, claims_file, description, final_trimester_code, srv_year, outcome, pdx,
      trimester_code_clean, newborn_flag, multiple_flag, loss_flag, labor_delivery_flag
    )
}

#' Modify Outcome Using Hierarchy5 Post-Term Conditions
#'
#' This function modifies the outcome of the given data based on the postterm conditions
#' in the hierarchy5.
#'
#' @param data A data frame containing the input data.
#' @return A data frame with the modified outcome based on the postterm conditions.
#' @noRd
modify_outcome_using_hierarchy5_postterm_conditions <- function(data) {
  data %>%
    mutate(
      min_gap = suppressWarnings(apply(select(., starts_with("gap")), MARGIN = 1, FUN = min, na.rm = TRUE)),
      min_gap = if_else(is.infinite(min_gap), NA_real_, min_gap),
      outcome = case_when(
        !(outcome %in% c("Term birth", "Preterm birth")) & min_gap >= 0 & min_gap <= 60 ~ "Post-term birth",
        TRUE ~ outcome
      )
    ) %>%
    select(
      enrolid, svcdate, code, claims_file, description, final_trimester_code, srv_year, outcome, pdx,
      trimester_code_clean, newborn_flag, multiple_flag, loss_flag, labor_delivery_flag
    )
}
