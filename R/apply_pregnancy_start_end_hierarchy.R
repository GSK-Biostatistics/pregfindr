#' Apply Pregnancy Start and End Hierarchy to Patients
#'
#' This function estimates the pregnancy start and end dates for Commercial Claims or Medicaid MarketScan patients and applies hierarchy logic to resolve conflicts.
#' It processes the data through multiple stages, resolving conflicts for ectopic, abortion, stillbirth, preterm, delivery, unclassified loss, postpartum, and trimester outcomes.
#'
#' @param outcome_hierarchy_final A data frame containing patient data with variables such as enrolid, svcdate, code, outcome, trimester_code_clean, and others.
#' @import sqldf
#' @import dplyr
#' @return A data frame with the applied pregnancy start and end hierarchy logic, containing variables such as enrolid, svcdate, code, outcome, trimester_code_clean, and others.
#' @examples
#' \dontrun{
#' trimester_hierarchy_final <- apply_trimester_hierarchy(patients)
#' outcome_hierarchy_final <- apply_outcome_hierarchy(trimester_hierarchy_final)
#' pregnancy_data_cleaned <- apply_pregnancy_start_end_hierarchy(outcome_hierarchy_final)
#' }
#' @export
apply_pregnancy_start_end_hierarchy <- function(outcome_hierarchy_final) {
  # Remove duplicates from outcome hierarchy results
  outcome_hierarchy_final_nodup <- remove_duplicates_from_outcome_hierarchy_results(outcome_hierarchy_final)
  
  # Use Z3A/P072/7652 codes to calculate start of pregnancy term and select and order the relevant columns
  pregnancy_start_date_info_sorted <- calculate_pregnancy_start(outcome_hierarchy_final_nodup) %>%
    select(enrolid, gestation_days, svcdate, preg_start, preg_start_char) %>%
    distinct() %>%
    arrange(enrolid, desc(as.numeric(svcdate)), desc(gestation_days), preg_start_char)

  pregnancy_start_date_info_unique <- pregnancy_start_date_info_sorted %>%
    distinct(enrolid, preg_start_char, .keep_all = TRUE)
  pregnancy_start_date_info_final <- resolve_gestation_term_conflicts(pregnancy_start_date_info_unique)
  
  # Ectopic
  query <- "select a.enrolid,a.svcdate, a.code, a.outcome, a.trimester_code_clean,b.preg_start,b.z3a_p07_date
              from outcome_hierarchy_final_nodup a left join
              pregnancy_start_date_info_final b on a.enrolid = b.enrolid and a.svcdate >= b.preg_start and a.svcdate <= b.z3a_p07_date"
  outcome_hierarchy_improved_for_ectopic <- sqldf(query) %>%
    as_tibble() %>%
    rename(
      z3a_p07_start_dt = preg_start,
      z3a_p07_last_dt = z3a_p07_date
    )
  
  ectopic_final <- resolve_outcome_conflicts(data = outcome_hierarchy_improved_for_ectopic,
                                             outcome_type = "ectopic",
                                             outcomes = "Ectopic pregnancy",
                                             gap_value = 56)
  # Abortion
  query <- "select a.*,b.ectopic_start_dt,b.ectopic_last_dt
              from outcome_hierarchy_improved_for_ectopic a left join
              ectopic_final b on a.enrolid = b.enrolid and a.svcdate >= b.ectopic_start_dt and a.svcdate <= b.ectopic_last_dt"
  
  outcome_hierarchy_improved_for_abortion <- sqldf(query)
  
  abortion_final <- resolve_outcome_conflicts(data = outcome_hierarchy_improved_for_abortion,
                                              outcome_type = "abortion",
                                              outcomes = c("Induced abortion", "Spontaneous abortion"),
                                              gap_value = 70)
  # Stillbirth
  query <- "select a.*,b.abortion_start_dt,b.abortion_last_dt
                from outcome_hierarchy_improved_for_abortion a left join
                abortion_final b on a.enrolid = b.enrolid and a.svcdate >= b.abortion_start_dt and a.svcdate <= b.abortion_last_dt"
  
  outcome_hierarchy_improved_for_stillbirth <- sqldf(query)
  
  stillbirth_final <- resolve_outcome_conflicts(data = outcome_hierarchy_improved_for_abortion,
                                                outcome_type = "stillbirth",
                                                outcomes = c("Stillbirth"),
                                                gap_value = 194)
  # Preterm
  query <- "select a.*,b.stillbirth_start_dt,b.stillbirth_last_dt
              from outcome_hierarchy_improved_for_stillbirth a left join
              stillbirth_final b on a.enrolid = b.enrolid and a.svcdate >= b.stillbirth_start_dt and a.svcdate <= b.stillbirth_last_dt"
  
  outcome_hierarchy_improved_for_preterm <- sqldf(query)
  
  preterm_final <- resolve_outcome_conflicts_for_preterm(outcome_hierarchy_improved_for_preterm)
  
  # All delivery
  query <- "select a.*,b.preterm_start_dt,b.preterm_last_dt
              from outcome_hierarchy_improved_for_preterm a left join
              preterm_final b on a.enrolid = b.enrolid and a.svcdate >= b.preterm_start_dt and a.svcdate <= b.preterm_last_dt"
  
  outcome_hierarchy_improved_for_all_delivery <- sqldf(query)
  all_delivery_final <- resolve_outcome_conflicts(data = outcome_hierarchy_improved_for_all_delivery,
                                                  outcome_type = "delivery",
                                                  outcomes = c("Cesarean section", "Delivery", "Post-term birth", "Post-term birth*", "Term birth"),
                                                  gap_value = 270)
  # Unclassified loss
  query <- "select a.*,b.delivery_start_dt,b.delivery_last_dt
              from outcome_hierarchy_improved_for_all_delivery a left join
              all_delivery_final b on a.enrolid = b.enrolid and a.svcdate >= b.delivery_start_dt and a.svcdate <= b.delivery_last_dt"
  
  outcome_hierarchy_improved_for_unclassified_loss <- sqldf(query)
  unclass_loss_final <- resolve_outcome_conflicts(data = outcome_hierarchy_improved_for_unclassified_loss,
                                                  outcome_type = "unclass_loss",
                                                  outcomes = c("Unclassified pregnancy loss"),
                                                  gap_value = 70)
  # Postpartum
  query <- "select a.*,b.unclass_loss_start_dt,b.unclass_loss_last_dt
              from outcome_hierarchy_improved_for_unclassified_loss a left join
              unclass_loss_final b on a.enrolid = b.enrolid and a.svcdate >= b.unclass_loss_start_dt and a.svcdate <= b.unclass_loss_last_dt"
  
  outcome_hierarchy_improved_for_postpartum <- sqldf(query)
  postpartum_final <- resolve_outcome_conflicts(data = outcome_hierarchy_improved_for_unclassified_loss,
                                                outcome_type = "postpartum",
                                                outcomes = c("Postpartum"),
                                                gap_value = 270,
                                                arrange_desc = FALSE)
  # trimester3
  query <- "select a.*,b.postpartum_start_dt,b.postpartum_last_dt
              from outcome_hierarchy_improved_for_postpartum a left join
              postpartum_final b on a.enrolid = b.enrolid and a.svcdate >= b.postpartum_start_dt and a.svcdate <= b.postpartum_last_dt"
  
  outcome_hierarchy_for_trimester_3 <- sqldf(query)
  trimester3_final <- resolve_trimester_conflicts(outcome_hierarchy_for_trimester_3, trimester = 3)
  
  # trimester2
  query <- "select a.*,b.trimester3_start_dt,b.trimester3_last_dt
              from outcome_hierarchy_for_trimester_3 a left join
              trimester3_final b on a.enrolid = b.enrolid and a.svcdate >= b.trimester3_start_dt and a.svcdate <= b.trimester3_last_dt"
  
  outcome_hierarchy_for_trimester_2 <- sqldf(query)
  trimester2_final <- resolve_trimester_conflicts(outcome_hierarchy_for_trimester_2, trimester = 2)
  
  # trimester1
  query <- "select a.*,b.trimester2_start_dt,b.trimester2_last_dt
              from outcome_hierarchy_for_trimester_2 a left join
              trimester2_final b on a.enrolid = b.enrolid and a.svcdate >= b.trimester2_start_dt and a.svcdate <= b.trimester2_last_dt"
  
  outcome_hierarchy_for_trimester_1 <- sqldf(query)
  trimester1_final <- resolve_trimester_conflicts(outcome_hierarchy_for_trimester_1, trimester = 1, arrange_desc = FALSE)
  
  # trimester4
  query <- "select a.*,b.trimester1_start_dt,b.trimester1_last_dt
              from outcome_hierarchy_for_trimester_1 a left join
              trimester1_final b on a.enrolid = b.enrolid and a.svcdate >= b.trimester1_start_dt and a.svcdate <= b.trimester1_last_dt"
  
  outcome_hierarchy_for_trimester_4 <- sqldf(query)
  trimester4_final <- resolve_trimester_conflicts(outcome_hierarchy_for_trimester_4, trimester = 4)
  
  # trimester5
  query <- "select a.*,b.trimester4_start_dt,b.trimester4_last_dt
              from outcome_hierarchy_for_trimester_4 a left join
              trimester4_final b on a.enrolid = b.enrolid and a.svcdate >= b.trimester4_start_dt and a.svcdate <= b.trimester4_last_dt"
  
  outcome_hierarchy_for_trimester_5 <- sqldf(query)
  trimester5_final <- resolve_trimester_conflicts(outcome_hierarchy_for_trimester_5, trimester = 5)
  
  # trimester8
  query <- "select a.*,b.trimester5_start_dt,b.trimester5_last_dt
              from outcome_hierarchy_for_trimester_5 a left join
              trimester5_final b on a.enrolid = b.enrolid and a.svcdate >= b.trimester5_start_dt and a.svcdate <= b.trimester5_last_dt"
  
  outcome_hierarchy_for_trimester_8 <- sqldf(query)
  trimester8_final <- resolve_trimester_conflicts(outcome_hierarchy_for_trimester_8, trimester = 8, arrange_desc = FALSE)
  
  # Final
  query <- "select a.*,b.trimester8_start_dt,b.trimester8_last_dt
              from outcome_hierarchy_for_trimester_8 a left join
              trimester8_final b on a.enrolid = b.enrolid and a.svcdate >= b.trimester8_start_dt and a.svcdate <= b.trimester8_last_dt"
  
  outcome_hierarchy_for_start_end_final <- sqldf(query)
  pregnancy_data_cleaned <- clean_up_pregnancy_start_end_results(outcome_hierarchy_for_start_end_final)
  
  # Garbage collection -------
  gc()
  return(pregnancy_data_cleaned)
}


# Start and end date derivation helpers -------------

#' Remove Duplicates from Outcome Hierarchy Results
#'
#' This function removes duplicates from the outcome hierarchy results.
#'
#' @param outcome_hierarchy_final A data frame containing the outcome hierarchy results.
#'
#' @return A data frame with duplicates removed, sorted by enrolid and svcdate.
#'
#' @noRd
remove_duplicates_from_outcome_hierarchy_results <- function(outcome_hierarchy_final) {
  outcome_hierarchy_final %>%
    mutate(enrolid = as.numeric(enrolid)) %>%
    select(enrolid, svcdate, code, outcome, trimester_code_clean) %>%
    distinct() %>%
    arrange(enrolid, svcdate)
}


#' Resolve gestation term conflicts
#'
#' Resolves conflicts in gestation terms for patients with overlapping gestation periods.
#' It identifies overlapping gestation terms for the same patient ID, counts the number of overlapping terms,
#' and resolves the conflicts by using the gaps between service dates and pregnancy start dates.
#'
#' @param pregnancy_start_date_info_unique A data frame containing unique pregnancy start date information.
#' @return A data frame with resolved gestation term conflicts.
#' @noRd
resolve_gestation_term_conflicts <- function(pregnancy_start_date_info_unique) {
  # Identify overlapping gestation terms for the same patient id
  pregnancy_start_date_info_dups <- pregnancy_start_date_info_unique %>%
    group_by(enrolid) %>%
    mutate(dups = n()) %>%
    select(enrolid, dups) %>%
    distinct()
  # Count the number of overlapping gestation terms for the same patient id
  pregnancy_start_date_info_count <- pregnancy_start_date_info_unique %>%
    left_join(pregnancy_start_date_info_dups, by = c("enrolid" = "enrolid")) %>%
    arrange(enrolid)
  # Store data for patients without overlapping gestation terms
  pregnancy_start_date_info_nodups <- pregnancy_start_date_info_count %>%
    filter(dups == 1) %>%
    select(-dups)
  # Resolve overlapping gestation conflicts
  pregnancy_start_date_info_dups <- pregnancy_start_date_info_count %>%
    filter(dups > 1) %>%
    group_by(enrolid) %>%
    mutate(seq_id = row_number()) %>%
    group_by(enrolid) %>% # TODO remove
    mutate(
      ref_date1 = lag(preg_start, n = 1),
      ref_date2 = lag(preg_start, n = 2),
      ref_date3 = lag(preg_start, n = 3),
      ref_date4 = lag(preg_start, n = 4),
      ref_date5 = lag(preg_start, n = 5),
      ref_date6 = lag(preg_start, n = 6),
      ref_date7 = lag(preg_start, n = 7),
      ref_date8 = lag(preg_start, n = 8),
      ref_date9 = lag(preg_start, n = 9),
      ref_date10 = lag(preg_start, n = 10),
      ref_date11 = lag(preg_start, n = 11),
      ref_date12 = lag(preg_start, n = 12),
      ref_date13 = lag(preg_start, n = 13),
      ref_date14 = lag(preg_start, n = 14)
    ) %>%
    mutate(
      gap1 = abs(as.numeric(preg_start - ref_date1)),
      gap2 = abs(as.numeric(preg_start - ref_date2)),
      gap3 = abs(as.numeric(preg_start - ref_date3)),
      gap4 = abs(as.numeric(preg_start - ref_date4)),
      gap5 = abs(as.numeric(preg_start - ref_date5)),
      gap6 = abs(as.numeric(preg_start - ref_date6)),
      gap7 = abs(as.numeric(preg_start - ref_date7)),
      gap8 = abs(as.numeric(preg_start - ref_date8)),
      gap9 = abs(as.numeric(preg_start - ref_date9)),
      gap10 = abs(as.numeric(preg_start - ref_date10)),
      gap11 = abs(as.numeric(preg_start - ref_date11)),
      gap12 = abs(as.numeric(preg_start - ref_date12)),
      gap13 = abs(as.numeric(preg_start - ref_date13)),
      gap14 = abs(as.numeric(preg_start - ref_date14))
    ) %>%
    mutate(flag = case_when(
      (0 <= gap1 & gap1 <= 45) | (0 <= gap2 & gap2 <= 45) | (0 <= gap3 & gap3 <= 45) |
        (0 <= gap4 & gap4 <= 45) | (0 <= gap5 & gap5 <= 45) | (0 <= gap6 & gap6 <= 45) |
        (0 <= gap7 & gap7 <= 45) | (0 <= gap8 & gap8 <= 45) | (0 <= gap9 & gap9 <= 45) |
        (0 <= gap10 & gap10 <= 45) | (0 <= gap11 & gap11 <= 45) | (0 <= gap12 & gap12 <= 45) |
        (0 <= gap13 & gap13 <= 45) | (0 <= gap14 & gap14 <= 45) ~ 0,
      TRUE ~ 1
    )) %>%
    filter(flag == 1) %>%
    select(enrolid, gestation_days, svcdate, preg_start, preg_start_char) %>%
    group_by(enrolid) %>%
    mutate(seq_id = row_number()) %>%
  # below steps take 3mins to run!
  group_by(enrolid) %>%
    mutate(
      ref_srv_date1 = lag(svcdate, n = 1),
      ref_srv_date2 = lag(svcdate, n = 2),
      ref_srv_date3 = lag(svcdate, n = 3),
      ref_srv_date4 = lag(svcdate, n = 4),
      ref_srv_date5 = lag(svcdate, n = 5),
      ref_srv_date6 = lag(svcdate, n = 6),
      ref_srv_date7 = lag(svcdate, n = 7),
      ref_srv_date8 = lag(svcdate, n = 8),
      ref_srv_date9 = lag(svcdate, n = 9),
      ref_srv_date10 = lag(svcdate, n = 10),
      ref_srv_date11 = lag(svcdate, n = 11),
      ref_srv_date12 = lag(svcdate, n = 12),
      ref_srv_date13 = lag(svcdate, n = 13),
      ref_srv_date14 = lag(svcdate, n = 14)
    ) %>%
    mutate(
      gap1 = abs(as.numeric(svcdate - ref_srv_date1)),
      gap2 = abs(as.numeric(svcdate - ref_srv_date2)),
      gap3 = abs(as.numeric(svcdate - ref_srv_date3)),
      gap4 = abs(as.numeric(svcdate - ref_srv_date4)),
      gap5 = abs(as.numeric(svcdate - ref_srv_date5)),
      gap6 = abs(as.numeric(svcdate - ref_srv_date6)),
      gap7 = abs(as.numeric(svcdate - ref_srv_date7)),
      gap8 = abs(as.numeric(svcdate - ref_srv_date8)),
      gap9 = abs(as.numeric(svcdate - ref_srv_date9)),
      gap10 = abs(as.numeric(svcdate - ref_srv_date10)),
      gap11 = abs(as.numeric(svcdate - ref_srv_date11)),
      gap12 = abs(as.numeric(svcdate - ref_srv_date12)),
      gap13 = abs(as.numeric(svcdate - ref_srv_date13)),
      gap14 = abs(as.numeric(svcdate - ref_srv_date14))
    ) %>%
    mutate(flag = case_when(
      (0 <= gap1 & gap1 <= 30) | (0 <= gap2 & gap2 <= 30) | (0 <= gap3 & gap3 <= 30) |
        (0 <= gap4 & gap4 <= 30) | (0 <= gap5 & gap5 <= 30) | (0 <= gap6 & gap6 <= 30) |
        (0 <= gap7 & gap7 <= 30) | (0 <= gap8 & gap8 <= 30) | (0 <= gap9 & gap9 <= 30) |
        (0 <= gap10 & gap10 <= 30) | (0 <= gap11 & gap11 <= 30) | (0 <= gap12 & gap12 <= 30) |
        (0 <= gap13 & gap13 <= 30) | (0 <= gap14 & gap14 <= 30) ~ 0,
      TRUE ~ 1
    )) %>%
    filter(flag == 1) %>%
    select(enrolid, gestation_days, svcdate, preg_start, preg_start_char) %>%
    group_by(enrolid) %>%
    mutate(seq_id = row_number()) %>%
    mutate(preg_start = if_else(is.na(preg_start), "", as.character(preg_start))) %>%
    group_by(enrolid) %>%
    mutate(
      ref_date1 = lag(preg_start, n = 1),
      ref_date2 = lag(preg_start, n = 2),
      ref_date3 = lag(preg_start, n = 3),
      ref_date4 = lag(preg_start, n = 4),
      ref_date5 = lag(preg_start, n = 5),
      ref_date6 = lag(preg_start, n = 6),
      ref_date7 = lag(preg_start, n = 7),
      ref_date8 = lag(preg_start, n = 8),
      ref_date9 = lag(preg_start, n = 9),
      ref_date10 = lag(preg_start, n = 10),
      ref_date11 = lag(preg_start, n = 11),
      ref_date12 = lag(preg_start, n = 12),
      ref_date13 = lag(preg_start, n = 13),
      ref_date14 = lag(preg_start, n = 14),
      ref_srv_date1 = lag(svcdate, n = 1),
      ref_srv_date2 = lag(svcdate, n = 2),
      ref_srv_date3 = lag(svcdate, n = 3),
      ref_srv_date4 = lag(svcdate, n = 4),
      ref_srv_date5 = lag(svcdate, n = 5),
      ref_srv_date6 = lag(svcdate, n = 6),
      ref_srv_date7 = lag(svcdate, n = 7),
      ref_srv_date8 = lag(svcdate, n = 8),
      ref_srv_date9 = lag(svcdate, n = 9),
      ref_srv_date10 = lag(svcdate, n = 10),
      ref_srv_date11 = lag(svcdate, n = 11),
      ref_srv_date12 = lag(svcdate, n = 12),
      ref_srv_date13 = lag(svcdate, n = 13),
      ref_srv_date14 = lag(svcdate, n = 14),
      ref_date1 = if_else(is.na(ref_date1), "", ref_date1),
      ref_srv_date1 = if_else(is.na(ref_srv_date1), "", as.character(ref_srv_date1))
    ) %>%
    mutate(flag = case_when(
      (ref_date1 <= svcdate) & (svcdate <= ref_srv_date1) | (ref_date2 <= svcdate) & (svcdate <= ref_srv_date2) |
        (ref_date3 <= svcdate) & (svcdate <= ref_srv_date3) | (ref_date4 <= svcdate) & (svcdate <= ref_srv_date4) |
        (ref_date5 <= svcdate) & (svcdate <= ref_srv_date5) | (ref_date6 <= svcdate) & (svcdate <= ref_srv_date6) |
        (ref_date7 <= svcdate) & (svcdate <= ref_srv_date7) | (ref_date8 <= svcdate) & (svcdate <= ref_srv_date8) |
        (ref_date9 <= svcdate) & (svcdate <= ref_srv_date9) | (ref_date10 <= svcdate) & (svcdate <= ref_srv_date10) |
        (ref_date11 <= svcdate) & (svcdate <= ref_srv_date11) | (ref_date12 <= svcdate) & (svcdate <= ref_srv_date12) |
        (ref_date13 <= svcdate) & (svcdate <= ref_srv_date13) | (ref_date14 <= svcdate) & (svcdate <= ref_srv_date14) |
        
        (ref_date1 <= preg_start) & (preg_start <= ref_srv_date1) | (ref_date2 <= preg_start) & (preg_start <= ref_srv_date2) |
        (ref_date3 <= preg_start) & (preg_start <= ref_srv_date3) | (ref_date4 <= preg_start) & (preg_start <= ref_srv_date4) |
        (ref_date5 <= preg_start) & (preg_start <= ref_srv_date5) | (ref_date6 <= preg_start) & (preg_start <= ref_srv_date6) |
        (ref_date7 <= preg_start) & (preg_start <= ref_srv_date7) | (ref_date8 <= preg_start) & (preg_start <= ref_srv_date8) |
        (ref_date9 <= preg_start) & (preg_start <= ref_srv_date9) | (ref_date10 <= preg_start) & (preg_start <= ref_srv_date10) |
        (ref_date11 <= preg_start) & (preg_start <= ref_srv_date11) | (ref_date12 <= preg_start) & (preg_start <= ref_srv_date12) |
        (ref_date13 <= preg_start) & (preg_start <= ref_srv_date13) | (ref_date14 <= preg_start) & (preg_start <= ref_srv_date14)
      ~ 0,
      TRUE ~ 1
    )) %>%
    filter(flag == 1) %>%
    mutate(preg_start = as.Date(preg_start)) %>%
    select(enrolid, gestation_days, svcdate, preg_start, preg_start_char)
  
  # Combine the data for patients without overlapping gestation terms with the resolved conflicts
  bind_rows(pregnancy_start_date_info_nodups, pregnancy_start_date_info_dups) %>%
    rename(z3a_p07_date = svcdate) %>%
    mutate(enrolid = as.numeric(enrolid))
}


#' Calculate start of pregnancy term using Z3A/P072/7652 codes
#'
# The codes mentioned (Z3A, P072, and 7652) are related to pregnancy and gestational age:
#   1. Z3A: This is an ICD-10 code that represents "weeks of gestation." It is used to indicate the specific week of pregnancy at the time of a healthcare encounter.
#   2. P072: This is an ICD-10 code that represents "extremely low birth weight newborn, less than 1000 grams." It is used to classify newborns with a birth weight of less than 1000 grams.
#   3. 7652: This is an ICD-9 code that represents "less than 37 completed weeks of gestation." It is used to classify preterm births.
#  By determining the gestational age at the time of a healthcare encounter (using the Z3A code) or the birth weight of a newborn (using the P072 or 7652 codes), we can estimate the start of the pregnancy term.
#
#' This function performs the following steps:
#' 1. Filters the data frame based on the following conditions:
#'    - The `code` starts with "Z3A" and is not in the list of excluded codes
#'    - The `code` is in the list of "P07" codes
#'    - The `code` is in the list of "7652" codes
#' 2. Creates a new column `gestation_days` by matching the `code` with the corresponding gestation days
#' 3. Calculates the pregnancy start date (`preg_start`) by subtracting the calculated gestation days from the service date
#' 4. Extracts the pregnancy start month (`preg_start_month`) and year (`preg_start_year`) from the calculated pregnancy start date
#' 5. Creates a character representation of the pregnancy start date (`preg_start_char`) by concatenating the month and year, or sets it to `NA_character_` if the `preg_start` is missing
#' 6. Returns the modified data frame with the new columns
#'
#' @param outcome_hierarchy_final_nodups A data frame containing the outcome hierarchy data.
#'
#' @return A modified data frame with the new columns.
#' @noRd
calculate_pregnancy_start <- function(outcome_hierarchy_final_nodups) {
  outcome_hierarchy_final_nodups %>%
    filter(startsWith(code, "Z3A") & !(code %in% c("Z3A00", "Z3A", "Z3A0", "Z3A01", "Z3A1", "Z3A2", "Z3A3", "Z3A4")) |
             code %in% c(
               "P0739", "P0738", "P0737", "P0736", "P0735", "P0734", "P0733", "P0732",
               "P0731", "P0726", "P0725", "P0724", "P0723", "P0722", "P0721"
             ) |
             code %in% c("76522", "76523", "76524", "76525", "76526", "76527", "76528")) %>%
    mutate(
      gestation_days = case_when(
        code == "Z3A08" ~ 56,
        code == "Z3A09" ~ 63,
        code == "Z3A10" ~ 70,
        code == "Z3A11" ~ 77,
        code == "Z3A12" ~ 84,
        code == "Z3A13" ~ 91,
        code == "Z3A14" ~ 98,
        code == "Z3A15" ~ 105,
        code == "Z3A16" ~ 112,
        code == "Z3A17" ~ 119,
        code == "Z3A18" ~ 126,
        code == "Z3A19" ~ 133,
        code == "Z3A20" ~ 140,
        code == "Z3A21" ~ 147,
        code == "Z3A22" ~ 154,
        code == "P0722" ~ 161,
        code == "Z3A23" ~ 161,
        code == "76522" ~ 168,
        code == "P0723" ~ 168,
        code == "Z3A24" ~ 168,
        code == "P0724" ~ 175,
        code == "Z3A25" ~ 175,
        code == "76523" ~ 182,
        code == "P0725" ~ 182,
        code == "Z3A26" ~ 182,
        code == "P0726" ~ 189,
        code == "Z3A27" ~ 189,
        code == "76524" ~ 196,
        code == "P0731" ~ 196,
        code == "Z3A28" ~ 196,
        code == "P0732" ~ 203,
        code == "Z3A29" ~ 203,
        code == "76525" ~ 210,
        code == "P0733" ~ 210,
        code == "Z3A30" ~ 210,
        code == "P0734" ~ 217,
        code == "Z3A31" ~ 217,
        code == "76526" ~ 224,
        code == "P0735" ~ 224,
        code == "Z3A32" ~ 224,
        code == "P0736" ~ 231,
        code == "Z3A33" ~ 231,
        code == "76527" ~ 238,
        code == "P0737" ~ 238,
        code == "Z3A34" ~ 238,
        code == "P0738" ~ 245,
        code == "Z3A35" ~ 245,
        code == "76528" ~ 252,
        code == "P0739" ~ 252,
        code == "Z3A36" ~ 252,
        code == "Z3A37" ~ 259,
        code == "Z3A38" ~ 266,
        code == "Z3A39" ~ 273,
        code == "Z3A40" ~ 280,
        code == "Z3A41" ~ 287,
        code == "Z3A42" ~ 294,
        code == "Z3A49" ~ 301
      ),
      preg_start = svcdate - gestation_days,
      preg_start_month = lubridate::month(preg_start),
      preg_start_year = lubridate::year(preg_start),
      preg_start_char = case_when(
        is.na(preg_start) == TRUE ~ NA_character_,
        TRUE ~ paste0(format(preg_start, "%m"), format(preg_start, "%Y"))
      )
    )
}

#' Use pregnancy outcomes to calculate start of pregnancy term
#' This function performs the following tasks:
# 1. It filters the joined data to get records with certain outcomes
# 2. It identifies duplicate records and calculates the count of pregnancies associated with the chosen outcomes for each enrolid.
# 3. It separates the records from the previous step into two groups: those with only one pregnancy and those with more than one.
# 4. For the group with more than one pregnancy, it identifies overlapping terms and removes them.
# 5. It combines the cleaned records from both groups and calculates the start date of the pregnancy term.
# 6. Finally, it returns the cleaned pregnancy data
#'
#' @param data data frame containing pregnancy outcome data
#' @param outcome_type atomic string, type of outcome used to determine start of pregnancy term
#' @param outcomes string vector, outcomes to match
#' @param gap_value atomic dbl
#' @param arrange_desc logical, if TRUE svcdate of conflicts is sorted in descending order 
#'
#' @return A data frame with resolved conflicts
#' @examples
#' \dontrun{
#'  resolve_outcome_conflicts(data = outcome_hierarchy_improved_for_ectopic,outcome_type = "ectopic",outcomes = "Ectopic pregnancy", gap_value = 56)
#' }
#'
#' @noRd
resolve_outcome_conflicts <- function(data, outcome_type, outcomes, gap_value, arrange_desc = TRUE) {
  outcome_data <- data %>%
    filter(outcome %in% outcomes)
  
  # Returns multiple dates per patient
  outcome_dup_count <- outcome_data %>%
    select(enrolid, svcdate) %>%
    distinct() %>%
    count(enrolid, name = "outcome_dups_per_enrolid")
  
  outcome_count <- outcome_data %>%
    left_join(outcome_dup_count, by = c("enrolid" = "enrolid")) %>%
    select(enrolid, svcdate, outcome_dups_per_enrolid) %>%
    distinct()
  
  outcome_nodups <- outcome_count %>%
    filter(outcome_dups_per_enrolid == 1) %>%
    select(enrolid, svcdate)
  
  # Identify Overlapping Terms & Delete Them
  
  if(arrange_desc == TRUE) {
    outcome_dups <- outcome_count %>%
      filter(outcome_dups_per_enrolid > 1) %>%
      arrange(desc(svcdate)) %>%
      arrange(enrolid)
  } else {
    outcome_dups <- outcome_count %>%
      filter(outcome_dups_per_enrolid > 1) %>%
      arrange(enrolid, svcdate) # difference is here
  }
  
  outcome_dups <- outcome_dups %>%
    group_by(enrolid) %>%
    mutate(seq_id = row_number()) %>%
    group_by(enrolid) %>%
    mutate(
      ref_date = lag(svcdate, n = 1),
      gap = abs(as.numeric(svcdate - ref_date)),
      flag = case_when(
        0 <= gap & gap <= gap_value ~ 0,
        TRUE ~ 1
      )
    ) %>%
    filter(flag == 1) %>%
    select(enrolid, svcdate)
  
  outcome_final <- bind_rows(outcome_nodups, outcome_dups) %>%
    group_by(enrolid) %>%
    mutate(outcome_start_dt = svcdate - gap_value) %>%
    rename(outcome_last_dt = svcdate) %>%
    arrange(enrolid, outcome_last_dt, outcome_start_dt) %>%
    distinct() %>%
    rename_with(
      ~ str_replace(., pattern = "outcome", replacement = outcome_type),
      starts_with("outcome")
    )
  
  return(outcome_final)
}

#' A custom version of `resolve_outcome_conflicts` for preterm outcomes
#' @param data data frame containing pregnancy outcome data
#' @param outcome_type atomic string, type of outcome used to determine start of pregnancy term
#' @param outcomes string vector, outcomes to match
#'
#' @return A data frame with resolved conflicts
#' @noRd
resolve_outcome_conflicts_for_preterm <- function(data, outcome_type = "preterm", outcomes = "Preterm birth") {
  outcome_data <- data %>%
    filter(outcome %in% outcomes)
  
  # Returns multiple ectopic dates per patient
  outcome_dup_count <- outcome_data %>%
    select(enrolid, svcdate) %>%
    distinct() %>%
    count(enrolid, name = "outcome_dups_per_enrolid")
  
  outcome_count <- outcome_data %>%
    left_join(outcome_dup_count, by = c("enrolid" = "enrolid")) %>%
    select(enrolid, svcdate, trimester_code_clean, outcome_dups_per_enrolid) %>%
    distinct()
  
  outcome_nodups <- outcome_count %>%
    filter(outcome_dups_per_enrolid == 1) %>%
    select(enrolid, svcdate, trimester_code_clean)
  
  # Identify Overlapping Terms & Delete Them
  outcome_dups <- outcome_count %>%
    filter(outcome_dups_per_enrolid > 1) %>%
    arrange(desc(svcdate)) %>%
    arrange(enrolid) %>%
    group_by(enrolid) %>%
    mutate(seq_id = row_number()) %>%
    group_by(enrolid) %>%
    mutate(
      ref_date = lag(svcdate, n = 1),
      gap = abs(as.numeric(svcdate - ref_date)),
      flag = case_when(
        0 <= gap & gap <= 182 ~ 0,
        TRUE ~ 1
      )
    ) %>%
    filter(flag == 1) %>%
    select(enrolid, svcdate, trimester_code_clean) %>%
    arrange(desc(svcdate)) %>%
    arrange(enrolid) %>%
    distinct() %>%
    group_by(enrolid) %>%
    mutate(seq_id = row_number()) %>%
    mutate(
      ref_date = lag(svcdate, n = 1),
      ref_tri_code = lag(trimester_code_clean, n = 1),
      gap = abs(as.numeric(svcdate - ref_date)),
      flag = case_when(
        ref_tri_code != 2 & 0 <= gap & gap <= 238 ~ 0,
        TRUE ~ 1
      )
    ) %>%
    filter(flag == 1) %>%
    select(enrolid, svcdate, trimester_code_clean)
  
  outcome_final <- bind_rows(outcome_nodups, outcome_dups) %>%
    group_by(enrolid) %>%
    mutate(
      outcome_start_dt =
        case_when(
          trimester_code_clean == 2 ~ as.numeric(svcdate) - 182,
          trimester_code_clean != 2 ~ as.numeric(svcdate) - 238,
          is.na(trimester_code_clean) ~ as.numeric(svcdate) - 238
        )
    ) %>%
    mutate(outcome_start_dt = as.Date(outcome_start_dt)) %>%
    rename(outcome_last_dt = svcdate) %>%
    arrange(enrolid, outcome_last_dt, outcome_start_dt) %>%
    distinct() %>%
    rename_with(
      ~ str_replace(., pattern = "outcome", replacement = outcome_type),
      starts_with("outcome")
    )
  
  return(outcome_final)
}

#' A custom version of `resolve_outcome_conflicts` for using using trimesters
#'
#' @param data data frame containing pregnancy outcome data
#' @param gap_value atomic dbl
#' @param trimester atomic dbl indicating trimester
#' @param arrange_desc logical, if TRUE svcdate of conflicts is sorted in descending order 
#'
#' @return A data frame with resolved conflicts
#' @examples
#' \dontrun{
#' resolve_trimester_conflicts(data = outcome_hierarchy_for_trimester_3,trimester = 3,outcomes = "Ectopic pregnancy", gap_value = 270)
#' }
#' @noRd
resolve_trimester_conflicts <- function(data, trimester, gap_value = 270, arrange_desc = TRUE) {
  stopifnot(trimester %in% c(3, 2, 1, 4, 5, 8))
  
  outcome_data <- data %>%
    filter(trimester_code_clean == trimester)
  
  # Returns multiple ectopic dates per patient
  outcome_dup_count <- outcome_data %>%
    select(enrolid, svcdate) %>%
    distinct() %>%
    count(enrolid, name = "outcome_dups_per_enrolid")
  
  outcome_count <- outcome_data %>%
    left_join(outcome_dup_count, by = c("enrolid" = "enrolid")) %>%
    select(enrolid, svcdate, outcome_dups_per_enrolid) %>%
    distinct()
  
  outcome_nodups <- outcome_count %>%
    filter(outcome_dups_per_enrolid == 1) %>%
    select(enrolid, svcdate)
  
  # Identify Overlapping Terms & Delete Them
  
  if(arrange_desc == TRUE) {
    outcome_dups <- outcome_count %>%
      filter(outcome_dups_per_enrolid > 1) %>%
      arrange(desc(svcdate)) %>%
      arrange(enrolid)
  } else {
    outcome_dups <- outcome_count %>%
      filter(outcome_dups_per_enrolid > 1) %>%
      arrange(enrolid, svcdate) # difference is here
  }
  
  outcome_dups <- outcome_dups %>%
    group_by(enrolid) %>%
    mutate(seq_id = row_number()) %>%
    group_by(enrolid) %>%
    mutate(
      ref_date = lag(svcdate, n = 1),
      gap = abs(as.numeric(svcdate - ref_date)),
      flag = case_when(
        0 <= gap & gap <= gap_value ~ 0,
        TRUE ~ 1
      )
    ) %>%
    filter(flag == 1) %>%
    select(enrolid, svcdate)
  
  outcome_final <- bind_rows(outcome_nodups, outcome_dups) %>%
    group_by(enrolid)
  
  if (trimester %in% c(3, 8)) {
    outcome_final <- outcome_final %>%
      mutate(outcome_start_dt = svcdate - gap_value) %>%
      rename(outcome_last_dt = svcdate)
  }
  
  if (trimester == 2) {
    outcome_final <- outcome_final %>%
      mutate(
        outcome_start_dt = svcdate - 135,
        outcome_last_dt = svcdate + 135
      ) %>%
      select(-svcdate)
  }
  
  if (trimester == 1) {
    outcome_final <- outcome_final %>%
      mutate(
        outcome_start_dt = svcdate - 56,
        outcome_last_dt = svcdate + 214
      ) %>%
      select(-svcdate)
  }
  
  if (trimester == 4) {
    outcome_final <- outcome_final %>%
      mutate(
        outcome_start_dt = svcdate - 96,
        outcome_last_dt = svcdate + 174
      ) %>%
      select(-svcdate)
  }
  
  if (trimester == 5) {
    outcome_final <- outcome_final %>%
      mutate(
        outcome_start_dt = svcdate - 202,
        outcome_last_dt = svcdate + 68
      ) %>%
      select(-svcdate)
  }
  
  outcome_final <- outcome_final %>%
    arrange(enrolid, outcome_last_dt, outcome_start_dt) %>%
    distinct() %>%
    rename_with(
      ~ str_replace(., pattern = "outcome", replacement = str_c("trimester", trimester)),
      starts_with("outcome")
    )
  
  return(outcome_final)
}

#' Clean Overlapping Pregnancy Terms using Hierarchy Logic
#'
#' This function performs the following tasks:
#' 1. Estimates the start and last date of pregnancy terms based on hierarchy logic.
#' 2. Deletes claims where no start/end date term can be identified.
#' 3. Identifies overlapping terms and removes them.
#' 4. Creates a final pregnancy term dataset with cleaned records.
#'
#' @param outcome_hierarchy_for_start_end_final A data frame containing the outcome hierarchy for start and end dates.
#'
#' @return A data frame with cleaned pregnancy terms, including enrolid, preg_start_dt, and preg_last_dt columns.
#' @noRd
clean_up_pregnancy_start_end_results <- function(outcome_hierarchy_for_start_end_final) {
  # Estimate Start Date
  preg_start_end <- outcome_hierarchy_for_start_end_final %>%
    lazy_dt() %>%
    mutate(
      preg_start_dt =
        case_when(
          !is.na(z3a_p07_start_dt) ~ z3a_p07_start_dt,
          !is.na(delivery_start_dt) & is.na(z3a_p07_start_dt) & !is.na(preterm_start_dt) ~ preterm_start_dt,
          !is.na(delivery_start_dt) & is.na(z3a_p07_start_dt) & !is.na(stillbirth_start_dt) & is.na(preterm_start_dt) ~ stillbirth_start_dt,
          !is.na(delivery_start_dt) & is.na(z3a_p07_start_dt) & !is.na(abortion_start_dt) & is.na(stillbirth_start_dt) & is.na(preterm_start_dt) ~ abortion_start_dt,
          !is.na(delivery_start_dt) & is.na(z3a_p07_start_dt) & !is.na(ectopic_start_dt) & is.na(abortion_start_dt) & is.na(stillbirth_start_dt) & is.na(preterm_start_dt) ~ ectopic_start_dt,
          is.na(delivery_start_dt) & is.na(z3a_p07_start_dt) & !is.na(ectopic_start_dt) ~ ectopic_start_dt,
          is.na(delivery_start_dt) & is.na(z3a_p07_start_dt) & is.na(ectopic_start_dt) & !is.na(abortion_start_dt) ~ abortion_start_dt,
          is.na(delivery_start_dt) & is.na(z3a_p07_start_dt) & is.na(ectopic_start_dt) & is.na(abortion_start_dt) & !is.na(stillbirth_start_dt) ~ stillbirth_start_dt,
          is.na(delivery_start_dt) & is.na(z3a_p07_start_dt) & is.na(ectopic_start_dt) & is.na(abortion_start_dt) & is.na(stillbirth_start_dt) & !is.na(preterm_start_dt) ~ preterm_start_dt,
          is.na(z3a_p07_start_dt) & !is.na(ectopic_start_dt) & is.na(abortion_start_dt) & is.na(stillbirth_start_dt) & is.na(preterm_start_dt) &
            is.na(delivery_start_dt) ~ ectopic_start_dt,
          is.na(z3a_p07_start_dt) & is.na(ectopic_start_dt) & !is.na(abortion_start_dt) & is.na(stillbirth_start_dt) & is.na(preterm_start_dt) &
            is.na(delivery_start_dt) ~ abortion_start_dt,
          is.na(z3a_p07_start_dt) & is.na(ectopic_start_dt) & is.na(abortion_start_dt) & !is.na(stillbirth_start_dt) & is.na(preterm_start_dt) &
            is.na(delivery_start_dt) ~ stillbirth_start_dt,
          is.na(z3a_p07_start_dt) & is.na(ectopic_start_dt) & is.na(abortion_start_dt) & is.na(stillbirth_start_dt) & !is.na(preterm_start_dt) &
            is.na(delivery_start_dt) ~ preterm_start_dt,
          is.na(z3a_p07_start_dt) & is.na(ectopic_start_dt) & is.na(abortion_start_dt) & is.na(stillbirth_start_dt) & is.na(preterm_start_dt) &
            !is.na(delivery_start_dt) ~ delivery_start_dt,
          is.na(z3a_p07_start_dt) & is.na(ectopic_start_dt) & is.na(abortion_start_dt) & is.na(stillbirth_start_dt) & is.na(preterm_start_dt) &
            is.na(delivery_start_dt) & !is.na(unclass_loss_start_dt) ~ unclass_loss_start_dt,
          is.na(z3a_p07_start_dt) & is.na(ectopic_start_dt) & is.na(abortion_start_dt) & is.na(stillbirth_start_dt) & is.na(preterm_start_dt) &
            is.na(delivery_start_dt) & is.na(unclass_loss_start_dt) & !is.na(trimester1_start_dt) ~ trimester1_start_dt,
          is.na(z3a_p07_start_dt) & is.na(ectopic_start_dt) & is.na(abortion_start_dt) & is.na(stillbirth_start_dt) & is.na(preterm_start_dt) &
            is.na(delivery_start_dt) & is.na(unclass_loss_start_dt) & is.na(trimester1_start_dt) & !is.na(trimester2_start_dt) ~ trimester2_start_dt,
          is.na(z3a_p07_start_dt) & is.na(ectopic_start_dt) & is.na(abortion_start_dt) & is.na(stillbirth_start_dt) & is.na(preterm_start_dt) &
            is.na(delivery_start_dt) & is.na(unclass_loss_start_dt) & is.na(trimester1_start_dt) & is.na(trimester2_start_dt) &
            !is.na(trimester3_start_dt) ~ trimester3_start_dt,
          is.na(z3a_p07_start_dt) & is.na(ectopic_start_dt) & is.na(abortion_start_dt) & is.na(stillbirth_start_dt) & is.na(preterm_start_dt) &
            is.na(delivery_start_dt) & is.na(unclass_loss_start_dt) & is.na(trimester1_start_dt) & is.na(trimester2_start_dt) & is.na(trimester3_start_dt) &
            !is.na(trimester4_start_dt) ~ trimester4_start_dt,
          is.na(z3a_p07_start_dt) & is.na(ectopic_start_dt) & is.na(abortion_start_dt) & is.na(stillbirth_start_dt) & is.na(preterm_start_dt) &
            is.na(delivery_start_dt) & is.na(unclass_loss_start_dt) & is.na(trimester1_start_dt) & is.na(trimester2_start_dt) & is.na(trimester3_start_dt) &
            is.na(trimester4_start_dt) & !is.na(trimester5_start_dt) ~ trimester5_start_dt,
          is.na(z3a_p07_start_dt) & is.na(ectopic_start_dt) & is.na(abortion_start_dt) & is.na(stillbirth_start_dt) & is.na(preterm_start_dt) &
            is.na(delivery_start_dt) & is.na(unclass_loss_start_dt) & is.na(trimester1_start_dt) & is.na(trimester2_start_dt) & is.na(trimester3_start_dt) &
            is.na(trimester4_start_dt) & is.na(trimester5_start_dt) & !is.na(postpartum_start_dt) ~ postpartum_start_dt,
          is.na(z3a_p07_start_dt) & is.na(ectopic_start_dt) & is.na(abortion_start_dt) & is.na(stillbirth_start_dt) & is.na(preterm_start_dt) &
            is.na(delivery_start_dt) & is.na(unclass_loss_start_dt) & is.na(trimester1_start_dt) & is.na(trimester2_start_dt) & is.na(trimester3_start_dt) &
            is.na(trimester4_start_dt) & is.na(trimester5_start_dt) & is.na(postpartum_start_dt) & !is.na(trimester8_start_dt) ~ trimester8_start_dt
        ),
      group_start =
        case_when(
          !is.na(preg_start_dt) & preg_start_dt == z3a_p07_start_dt ~ "01 z3a_p07",
          !is.na(preg_start_dt) & preg_start_dt == ectopic_start_dt ~ "02 ectopic",
          !is.na(preg_start_dt) & preg_start_dt == abortion_start_dt ~ "03 abortion",
          !is.na(preg_start_dt) & preg_start_dt == stillbirth_start_dt ~ "04 stillbirth",
          !is.na(preg_start_dt) & preg_start_dt == preterm_start_dt ~ "05 preterm",
          !is.na(preg_start_dt) & preg_start_dt == delivery_start_dt ~ "06 delivery",
          !is.na(preg_start_dt) & preg_start_dt == unclass_loss_start_dt ~ "07 unclass_loss",
          !is.na(preg_start_dt) & preg_start_dt == trimester1_start_dt ~ "08 trimester1",
          !is.na(preg_start_dt) & preg_start_dt == trimester2_start_dt ~ "09 trimester2",
          !is.na(preg_start_dt) & preg_start_dt == trimester3_start_dt ~ "10 trimester3",
          !is.na(preg_start_dt) & preg_start_dt == trimester4_start_dt ~ "11 trimester4",
          !is.na(preg_start_dt) & preg_start_dt == trimester5_start_dt ~ "12 trimester5",
          !is.na(preg_start_dt) & preg_start_dt == postpartum_start_dt ~ "13 postpartum",
          !is.na(preg_start_dt) & preg_start_dt == trimester8_start_dt ~ "14 trimester8"
        ),
      # Estimate Last Date
      preg_last_dt =
        case_when(
          !is.na(delivery_last_dt) ~ delivery_last_dt,
          is.na(delivery_last_dt) & !is.na(z3a_p07_last_dt) & !is.na(ectopic_last_dt) & z3a_p07_last_dt > ectopic_last_dt ~ z3a_p07_last_dt,
          is.na(delivery_last_dt) & !is.na(z3a_p07_last_dt) & !is.na(ectopic_last_dt) ~ ectopic_last_dt,
          is.na(delivery_last_dt) & !is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & !is.na(abortion_last_dt) & z3a_p07_last_dt > abortion_last_dt ~ z3a_p07_last_dt,
          is.na(delivery_last_dt) & !is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & !is.na(abortion_last_dt) ~ abortion_last_dt,
          is.na(delivery_last_dt) & !is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & !is.na(stillbirth_last_dt) &
            z3a_p07_last_dt > stillbirth_last_dt ~ z3a_p07_last_dt,
          is.na(delivery_last_dt) & !is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & !is.na(stillbirth_last_dt) ~ stillbirth_last_dt,
          is.na(delivery_last_dt) & !is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) &
            !is.na(unclass_loss_last_dt) & z3a_p07_last_dt > unclass_loss_last_dt ~ z3a_p07_last_dt,
          is.na(delivery_last_dt) & !is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) &
            !is.na(unclass_loss_last_dt) ~ unclass_loss_last_dt,
          is.na(delivery_last_dt) & !is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) &
            is.na(unclass_loss_last_dt) & !is.na(preterm_last_dt) & z3a_p07_last_dt > preterm_last_dt ~ z3a_p07_last_dt,
          is.na(delivery_last_dt) & !is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) &
            is.na(unclass_loss_last_dt) & !is.na(preterm_last_dt) ~ preterm_last_dt,
          is.na(delivery_last_dt) & !is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) &
            is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) & !is.na(trimester3_last_dt) & z3a_p07_last_dt > trimester3_last_dt ~ z3a_p07_last_dt,
          is.na(delivery_last_dt) & !is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) &
            is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) & !is.na(trimester3_last_dt) ~ trimester3_last_dt,
          is.na(delivery_last_dt) & !is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) &
            is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) & is.na(trimester3_last_dt) & !is.na(trimester2_last_dt) &
            z3a_p07_last_dt > trimester2_last_dt ~ z3a_p07_last_dt,
          is.na(delivery_last_dt) & !is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) &
            is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) & is.na(trimester3_last_dt) & !is.na(trimester2_last_dt) ~ trimester2_last_dt,
          is.na(delivery_last_dt) & !is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) &
            is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) & is.na(trimester3_last_dt) & is.na(trimester2_last_dt) & !is.na(trimester1_last_dt) &
            z3a_p07_last_dt > trimester1_last_dt ~ z3a_p07_last_dt,
          is.na(delivery_last_dt) & !is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) &
            is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) & is.na(trimester3_last_dt) & is.na(trimester2_last_dt) & !is.na(trimester1_last_dt) ~ trimester1_last_dt,
          is.na(delivery_last_dt) & !is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) &
            is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) & is.na(trimester3_last_dt) & is.na(trimester2_last_dt) & is.na(trimester1_last_dt) &
            !is.na(trimester4_last_dt) &
            z3a_p07_last_dt > trimester4_last_dt ~ z3a_p07_last_dt,
          is.na(delivery_last_dt) & !is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) &
            is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) & is.na(trimester3_last_dt) & is.na(trimester2_last_dt) & is.na(trimester1_last_dt) &
            !is.na(trimester4_last_dt) ~ trimester4_last_dt,
          is.na(delivery_last_dt) & !is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) &
            is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) & is.na(trimester3_last_dt) & is.na(trimester2_last_dt) & is.na(trimester1_last_dt) &
            is.na(trimester4_last_dt) & !is.na(trimester5_last_dt) &
            z3a_p07_last_dt > trimester5_last_dt ~ z3a_p07_last_dt,
          is.na(delivery_last_dt) & !is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) &
            is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) & is.na(trimester3_last_dt) & is.na(trimester2_last_dt) & is.na(trimester1_last_dt) &
            is.na(trimester4_last_dt) & !is.na(trimester5_last_dt) ~ trimester5_last_dt,
          is.na(delivery_last_dt) & !is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) &
            is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) & is.na(trimester3_last_dt) & is.na(trimester2_last_dt) & is.na(trimester1_last_dt) &
            is.na(trimester4_last_dt) & is.na(trimester5_last_dt) & !is.na(postpartum_last_dt) &
            z3a_p07_last_dt > postpartum_last_dt ~ z3a_p07_last_dt,
          is.na(delivery_last_dt) & !is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) &
            is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) & is.na(trimester3_last_dt) & is.na(trimester2_last_dt) & is.na(trimester1_last_dt) &
            is.na(trimester4_last_dt) & is.na(trimester5_last_dt) & !is.na(postpartum_last_dt) ~ postpartum_last_dt,
          is.na(delivery_last_dt) & !is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) &
            is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) & is.na(trimester3_last_dt) & is.na(trimester2_last_dt) & is.na(trimester1_last_dt) &
            is.na(trimester4_last_dt) & is.na(trimester5_last_dt) & is.na(postpartum_last_dt) & !is.na(trimester8_last_dt) &
            z3a_p07_last_dt > trimester8_last_dt ~ z3a_p07_last_dt,
          is.na(delivery_last_dt) & !is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) &
            is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) & is.na(trimester3_last_dt) & is.na(trimester2_last_dt) & is.na(trimester1_last_dt) &
            is.na(trimester4_last_dt) & is.na(trimester5_last_dt) & is.na(postpartum_last_dt) & !is.na(trimester8_last_dt) ~ trimester8_last_dt,
          is.na(delivery_last_dt) & is.na(z3a_p07_last_dt) & !is.na(ectopic_last_dt) ~ ectopic_last_dt,
          is.na(delivery_last_dt) & is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & !is.na(abortion_last_dt) ~ abortion_last_dt,
          is.na(delivery_last_dt) & is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & !is.na(stillbirth_last_dt) ~ stillbirth_last_dt,
          is.na(delivery_last_dt) & is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) & !is.na(unclass_loss_last_dt) ~ unclass_loss_last_dt,
          is.na(delivery_last_dt) & is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) & is.na(unclass_loss_last_dt) & !is.na(preterm_last_dt) ~ preterm_last_dt,
          is.na(z3a_p07_last_dt) & !is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) & is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) &
            is.na(delivery_last_dt) ~ ectopic_last_dt,
          is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & !is.na(abortion_last_dt) & is.na(stillbirth_last_dt) & is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) &
            is.na(delivery_last_dt) ~ abortion_last_dt,
          is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & !is.na(stillbirth_last_dt) & is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) &
            is.na(delivery_last_dt) ~ stillbirth_last_dt,
          is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) & !is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) &
            is.na(delivery_last_dt) ~ unclass_loss_last_dt,
          is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) & is.na(unclass_loss_last_dt) & !is.na(preterm_last_dt) &
            is.na(delivery_last_dt) ~ preterm_last_dt,
          is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) & is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) &
            !is.na(delivery_last_dt) ~ delivery_last_dt,
          is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) & is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) &
            is.na(delivery_last_dt) & !is.na(trimester3_last_dt) ~ trimester3_last_dt,
          is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) & is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) &
            is.na(delivery_last_dt) & is.na(trimester3_last_dt) & !is.na(trimester2_last_dt) ~ trimester2_last_dt,
          is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) & is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) &
            is.na(delivery_last_dt) & is.na(trimester3_last_dt) & is.na(trimester2_last_dt) & !is.na(trimester1_last_dt) ~ trimester1_last_dt,
          is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) & is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) &
            is.na(delivery_last_dt) & is.na(trimester3_last_dt) & is.na(trimester2_last_dt) & is.na(trimester1_last_dt) & !is.na(trimester4_last_dt) ~ trimester4_last_dt,
          is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) & is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) &
            is.na(delivery_last_dt) & is.na(trimester3_last_dt) & is.na(trimester2_last_dt) & is.na(trimester1_last_dt) & is.na(trimester4_last_dt) &
            !is.na(trimester5_last_dt) ~ trimester5_last_dt,
          is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) & is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) &
            is.na(delivery_last_dt) & is.na(trimester3_last_dt) & is.na(trimester2_last_dt) & is.na(trimester1_last_dt) & is.na(trimester4_last_dt) &
            is.na(trimester5_last_dt) & !is.na(postpartum_last_dt) ~ postpartum_last_dt,
          is.na(z3a_p07_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) & is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) &
            is.na(delivery_last_dt) & is.na(trimester3_last_dt) & is.na(trimester2_last_dt) & is.na(trimester1_last_dt) & is.na(trimester4_last_dt) &
            is.na(trimester5_last_dt) & is.na(postpartum_last_dt) & !is.na(trimester8_last_dt) ~ trimester8_last_dt,
          is.na(delivery_last_dt) & is.na(ectopic_last_dt) & is.na(abortion_last_dt) & is.na(stillbirth_last_dt) & is.na(unclass_loss_last_dt) & is.na(preterm_last_dt) &
            is.na(trimester3_last_dt) & is.na(trimester2_last_dt) & is.na(trimester1_last_dt) & is.na(trimester4_last_dt) &
            is.na(trimester5_last_dt) & is.na(postpartum_last_dt) & is.na(trimester8_last_dt) & !is.na(z3a_p07_last_dt) ~ z3a_p07_last_dt
        ),
      group_last =
        case_when(
          !is.na(preg_last_dt) & preg_last_dt == delivery_last_dt ~ "01 delivery",
          !is.na(preg_last_dt) & preg_last_dt == ectopic_last_dt ~ "02 ectopic",
          !is.na(preg_last_dt) & preg_last_dt == abortion_last_dt ~ "03 abortion",
          !is.na(preg_last_dt) & preg_last_dt == stillbirth_last_dt ~ "04 stillbirth",
          !is.na(preg_last_dt) & preg_last_dt == unclass_loss_last_dt ~ "05 unclass_loss",
          !is.na(preg_last_dt) & preg_last_dt == preterm_last_dt ~ "06 preterm",
          !is.na(preg_last_dt) & preg_last_dt == trimester3_last_dt ~ "07 trimester3",
          !is.na(preg_last_dt) & preg_last_dt == trimester2_last_dt ~ "08 trimester2",
          !is.na(preg_last_dt) & preg_last_dt == trimester1_last_dt ~ "09 trimester1",
          !is.na(preg_last_dt) & preg_last_dt == trimester4_last_dt ~ "10 trimester4",
          !is.na(preg_last_dt) & preg_last_dt == trimester5_last_dt ~ "11 trimester5",
          !is.na(preg_last_dt) & preg_last_dt == postpartum_last_dt ~ "12 postpartum",
          !is.na(preg_last_dt) & preg_last_dt == trimester8_last_dt ~ "13 trimester8",
          !is.na(preg_last_dt) & preg_last_dt == z3a_p07_last_dt ~ "14 z3a_p07"
        )
    ) %>%
    as_tibble()
  
  # Delete claims where no start/end date term can be identified
  
  preg_start_end_pt <- preg_start_end %>%
    filter(!is.na(preg_start_dt)) %>%
    select(enrolid, preg_start_dt, preg_last_dt, group_start, group_last) %>%
    arrange(desc(preg_last_dt)) %>%
    arrange(enrolid, group_last, group_start, preg_start_dt) %>%
    distinct()
  
  # Identify Overlapping Terms & Delete Them
  
  preg_start_end_pt2 <- preg_start_end_pt %>%
    group_by(enrolid) %>%
    mutate(seq_id = row_number())
  
  preg_start_end_pt3 <- lazy_dt(preg_start_end_pt2) %>%
    group_by(enrolid) %>%
    mutate(
      ref_start_date1 = lag(preg_start_dt, n = 1),
      ref_start_date2 = lag(preg_start_dt, n = 2),
      ref_start_date3 = lag(preg_start_dt, n = 3),
      ref_start_date4 = lag(preg_start_dt, n = 4),
      ref_start_date5 = lag(preg_start_dt, n = 5),
      ref_start_date6 = lag(preg_start_dt, n = 6),
      ref_start_date7 = lag(preg_start_dt, n = 7),
      ref_start_date8 = lag(preg_start_dt, n = 8),
      ref_start_date9 = lag(preg_start_dt, n = 9),
      ref_start_date10 = lag(preg_start_dt, n = 10),
      ref_start_date11 = lag(preg_start_dt, n = 11),
      ref_start_date12 = lag(preg_start_dt, n = 12),
      ref_start_date13 = lag(preg_start_dt, n = 13),
      ref_start_date14 = lag(preg_start_dt, n = 14),
      ref_last_date1 = lag(preg_last_dt, n = 1),
      ref_last_date2 = lag(preg_last_dt, n = 2),
      ref_last_date3 = lag(preg_last_dt, n = 3),
      ref_last_date4 = lag(preg_last_dt, n = 4),
      ref_last_date5 = lag(preg_last_dt, n = 5),
      ref_last_date6 = lag(preg_last_dt, n = 6),
      ref_last_date7 = lag(preg_last_dt, n = 7),
      ref_last_date8 = lag(preg_last_dt, n = 8),
      ref_last_date9 = lag(preg_last_dt, n = 9),
      ref_last_date10 = lag(preg_last_dt, n = 10),
      ref_last_date11 = lag(preg_last_dt, n = 11),
      ref_last_date12 = lag(preg_last_dt, n = 12),
      ref_last_date13 = lag(preg_last_dt, n = 13),
      ref_last_date14 = lag(preg_last_dt, n = 14)
    ) %>%
    mutate(flag = case_when(
      (ref_start_date1 <= preg_last_dt) & (preg_last_dt <= ref_last_date1) | (ref_start_date2 <= preg_last_dt) & (preg_last_dt <= ref_last_date2) |
        (ref_start_date3 <= preg_last_dt) & (preg_last_dt <= ref_last_date3) | (ref_start_date4 <= preg_last_dt) & (preg_last_dt <= ref_last_date4) |
        (ref_start_date5 <= preg_last_dt) & (preg_last_dt <= ref_last_date5) | (ref_start_date6 <= preg_last_dt) & (preg_last_dt <= ref_last_date6) |
        (ref_start_date7 <= preg_last_dt) & (preg_last_dt <= ref_last_date7) | (ref_start_date8 <= preg_last_dt) & (preg_last_dt <= ref_last_date8) |
        (ref_start_date9 <= preg_last_dt) & (preg_last_dt <= ref_last_date9) | (ref_start_date10 <= preg_last_dt) & (preg_last_dt <= ref_last_date10) |
        (ref_start_date11 <= preg_last_dt) & (preg_last_dt <= ref_last_date11) | (ref_start_date12 <= preg_last_dt) & (preg_last_dt <= ref_last_date12) |
        (ref_start_date13 <= preg_last_dt) & (preg_last_dt <= ref_last_date13) | (ref_start_date14 <= preg_last_dt) & (preg_last_dt <= ref_last_date14) |
        
        (ref_start_date1 <= preg_start_dt) & (preg_start_dt <= ref_last_date1) | (ref_start_date2 <= preg_start_dt) & (preg_start_dt <= ref_last_date2) |
        (ref_start_date3 <= preg_start_dt) & (preg_start_dt <= ref_last_date3) | (ref_start_date4 <= preg_start_dt) & (preg_start_dt <= ref_last_date4) |
        (ref_start_date5 <= preg_start_dt) & (preg_start_dt <= ref_last_date5) | (ref_start_date6 <= preg_start_dt) & (preg_start_dt <= ref_last_date6) |
        (ref_start_date7 <= preg_start_dt) & (preg_start_dt <= ref_last_date7) | (ref_start_date8 <= preg_start_dt) & (preg_start_dt <= ref_last_date8) |
        (ref_start_date9 <= preg_start_dt) & (preg_start_dt <= ref_last_date9) | (ref_start_date10 <= preg_start_dt) & (preg_start_dt <= ref_last_date10) |
        (ref_start_date11 <= preg_start_dt) & (preg_start_dt <= ref_last_date11) | (ref_start_date12 <= preg_start_dt) & (preg_start_dt <= ref_last_date12) |
        (ref_start_date13 <= preg_start_dt) & (preg_start_dt <= ref_last_date13) | (ref_start_date14 <= preg_start_dt) & (preg_start_dt <= ref_last_date14) |
        
        (preg_start_dt <= ref_start_date1) & (ref_start_date1 <= preg_last_dt) | (preg_start_dt <= ref_start_date2) & (ref_start_date2 <= preg_last_dt) |
        (preg_start_dt <= ref_start_date3) & (ref_start_date3 <= preg_last_dt) | (preg_start_dt <= ref_start_date4) & (ref_start_date4 <= preg_last_dt) |
        (preg_start_dt <= ref_start_date5) & (ref_start_date5 <= preg_last_dt) | (preg_start_dt <= ref_start_date6) & (ref_start_date6 <= preg_last_dt) |
        (preg_start_dt <= ref_start_date7) & (ref_start_date7 <= preg_last_dt) | (preg_start_dt <= ref_start_date8) & (ref_start_date8 <= preg_last_dt) |
        (preg_start_dt <= ref_start_date9) & (ref_start_date9 <= preg_last_dt) | (preg_start_dt <= ref_start_date10) & (ref_start_date10 <= preg_last_dt) |
        (preg_start_dt <= ref_start_date11) & (ref_start_date11 <= preg_last_dt) | (preg_start_dt <= ref_start_date12) & (ref_start_date12 <= preg_last_dt) |
        (preg_start_dt <= ref_start_date13) & (ref_start_date13 <= preg_last_dt) | (preg_start_dt <= ref_start_date14) & (ref_start_date14 <= preg_last_dt)
      
      ~ 0,
      TRUE ~ 1
    )) %>%
    filter(flag == 1) %>%
    select(enrolid, preg_start_dt, preg_last_dt, group_start, group_last) %>%
    arrange(enrolid, group_last, group_start, preg_start_dt, preg_last_dt)
  
  preg_start_end_pt3_1 <- preg_start_end_pt3 %>% 
    as_tibble()
  
  preg_start_end_pt4 <- preg_start_end_pt3_1 %>%
    group_by(enrolid) %>%
    mutate(seq_id = row_number())
  
  preg_start_end_pt5 <- lazy_dt(preg_start_end_pt4) %>%
    group_by(enrolid) %>%
    mutate(
      ref_start_date1 = lag(preg_start_dt, n = 1),
      ref_start_date2 = lag(preg_start_dt, n = 2),
      ref_start_date3 = lag(preg_start_dt, n = 3),
      ref_start_date4 = lag(preg_start_dt, n = 4),
      ref_start_date5 = lag(preg_start_dt, n = 5),
      ref_start_date6 = lag(preg_start_dt, n = 6),
      ref_start_date7 = lag(preg_start_dt, n = 7),
      ref_start_date8 = lag(preg_start_dt, n = 8),
      ref_start_date9 = lag(preg_start_dt, n = 9),
      ref_start_date10 = lag(preg_start_dt, n = 10),
      ref_start_date11 = lag(preg_start_dt, n = 11),
      ref_start_date12 = lag(preg_start_dt, n = 12),
      ref_start_date13 = lag(preg_start_dt, n = 13),
      ref_start_date14 = lag(preg_start_dt, n = 14),
      ref_last_date1 = lag(preg_last_dt, n = 1),
      ref_last_date2 = lag(preg_last_dt, n = 2),
      ref_last_date3 = lag(preg_last_dt, n = 3),
      ref_last_date4 = lag(preg_last_dt, n = 4),
      ref_last_date5 = lag(preg_last_dt, n = 5),
      ref_last_date6 = lag(preg_last_dt, n = 6),
      ref_last_date7 = lag(preg_last_dt, n = 7),
      ref_last_date8 = lag(preg_last_dt, n = 8),
      ref_last_date9 = lag(preg_last_dt, n = 9),
      ref_last_date10 = lag(preg_last_dt, n = 10),
      ref_last_date11 = lag(preg_last_dt, n = 11),
      ref_last_date12 = lag(preg_last_dt, n = 12),
      ref_last_date13 = lag(preg_last_dt, n = 13),
      ref_last_date14 = lag(preg_last_dt, n = 14)
    ) %>%
    mutate(flag = case_when(
      (ref_start_date1 <= preg_last_dt) & (preg_last_dt <= ref_last_date1) | (ref_start_date2 <= preg_last_dt) & (preg_last_dt <= ref_last_date2) |
        (ref_start_date3 <= preg_last_dt) & (preg_last_dt <= ref_last_date3) | (ref_start_date4 <= preg_last_dt) & (preg_last_dt <= ref_last_date4) |
        (ref_start_date5 <= preg_last_dt) & (preg_last_dt <= ref_last_date5) | (ref_start_date6 <= preg_last_dt) & (preg_last_dt <= ref_last_date6) |
        (ref_start_date7 <= preg_last_dt) & (preg_last_dt <= ref_last_date7) | (ref_start_date8 <= preg_last_dt) & (preg_last_dt <= ref_last_date8) |
        (ref_start_date9 <= preg_last_dt) & (preg_last_dt <= ref_last_date9) | (ref_start_date10 <= preg_last_dt) & (preg_last_dt <= ref_last_date10) |
        (ref_start_date11 <= preg_last_dt) & (preg_last_dt <= ref_last_date11) | (ref_start_date12 <= preg_last_dt) & (preg_last_dt <= ref_last_date12) |
        (ref_start_date13 <= preg_last_dt) & (preg_last_dt <= ref_last_date13) | (ref_start_date14 <= preg_last_dt) & (preg_last_dt <= ref_last_date14) |
        
        (ref_start_date1 <= preg_start_dt) & (preg_start_dt <= ref_last_date1) | (ref_start_date2 <= preg_start_dt) & (preg_start_dt <= ref_last_date2) |
        (ref_start_date3 <= preg_start_dt) & (preg_start_dt <= ref_last_date3) | (ref_start_date4 <= preg_start_dt) & (preg_start_dt <= ref_last_date4) |
        (ref_start_date5 <= preg_start_dt) & (preg_start_dt <= ref_last_date5) | (ref_start_date6 <= preg_start_dt) & (preg_start_dt <= ref_last_date6) |
        (ref_start_date7 <= preg_start_dt) & (preg_start_dt <= ref_last_date7) | (ref_start_date8 <= preg_start_dt) & (preg_start_dt <= ref_last_date8) |
        (ref_start_date9 <= preg_start_dt) & (preg_start_dt <= ref_last_date9) | (ref_start_date10 <= preg_start_dt) & (preg_start_dt <= ref_last_date10) |
        (ref_start_date11 <= preg_start_dt) & (preg_start_dt <= ref_last_date11) | (ref_start_date12 <= preg_start_dt) & (preg_start_dt <= ref_last_date12) |
        (ref_start_date13 <= preg_start_dt) & (preg_start_dt <= ref_last_date13) | (ref_start_date14 <= preg_start_dt) & (preg_start_dt <= ref_last_date14) |
        
        (preg_start_dt <= ref_start_date1) & (ref_start_date1 <= preg_last_dt) | (preg_start_dt <= ref_start_date2) & (ref_start_date2 <= preg_last_dt) |
        (preg_start_dt <= ref_start_date3) & (ref_start_date3 <= preg_last_dt) | (preg_start_dt <= ref_start_date4) & (ref_start_date4 <= preg_last_dt) |
        (preg_start_dt <= ref_start_date5) & (ref_start_date5 <= preg_last_dt) | (preg_start_dt <= ref_start_date6) & (ref_start_date6 <= preg_last_dt) |
        (preg_start_dt <= ref_start_date7) & (ref_start_date7 <= preg_last_dt) | (preg_start_dt <= ref_start_date8) & (ref_start_date8 <= preg_last_dt) |
        (preg_start_dt <= ref_start_date9) & (ref_start_date9 <= preg_last_dt) | (preg_start_dt <= ref_start_date10) & (ref_start_date10 <= preg_last_dt) |
        (preg_start_dt <= ref_start_date11) & (ref_start_date11 <= preg_last_dt) | (preg_start_dt <= ref_start_date12) & (ref_start_date12 <= preg_last_dt) |
        (preg_start_dt <= ref_start_date13) & (ref_start_date13 <= preg_last_dt) | (preg_start_dt <= ref_start_date14) & (ref_start_date14 <= preg_last_dt)
      
      ~ 0,
      TRUE ~ 1
    )) %>%
    filter(flag == 1) %>%
    select(enrolid, preg_start_dt, preg_last_dt)
  
  preg_start_end_pt5_1 <- preg_start_end_pt5 %>% as_tibble()
  
  # Create Final Pregnancy Term Dataset
  # Use one step to sort, remove duplicate records and create final output dateset directly
  
  preg_start_end_final <- preg_start_end_pt5_1 %>%
    arrange(desc(preg_last_dt)) %>%
    arrange(enrolid, preg_start_dt) %>%
    distinct()
  
  return(preg_start_end_final)
}