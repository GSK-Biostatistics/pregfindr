#' Apply Trimester Hierarchy to Patients
#'
#' Identify duplicate trimester codes for the same service day and apply hierarchy logic to resolve conflicts.
#' Group procedures performed on eligible women by enrolid and date of procedure.
#' Multiple conflicting trimester codes could occur on the same day, so the trimester group code for that service day needs to be reassigned based on the logic within the function.
#'
#' @param patients A data frame containing patient data with the format of the exported dataset extraction example dataset.
#' @return A data frame with the applied trimester hierarchy logic, containing variables such as enrolid, svcdate, code, claims_file, description, final_trimester_code, secondary_outcome, srv_year, outcome, pdx, and trimester_code_clean.
#'
#' @import fastDummies
#' @import dplyr
#' @importFrom tidyr replace_na
#' @importFrom stringr str_trim
#'
#' @examples
#' \dontrun{
#' patients <- arrow::read_parquet("patients.parquet")
#' trimester_hierarchy_final <- apply_trimester_hierarchy(patients)
#' }
#' @export
apply_trimester_hierarchy <- function(patients) {
tryCatch({

  # Apply trimester classification hierarchies -------------
  
  # Apply Hierarchy #1
  
  ## Turn final_trimester_code into dummy columns
  patients_trimester <- patients %>%
    mutate(trimester = factor(final_trimester_code, levels = c(1, 2, 3, 4, 5, 8, 9))) %>%
    fastDummies::dummy_cols("trimester") %>%
    select(-trimester)
  
  trimester_hierarchy1_list <- apply_trimester_hierarchy_1(
    pt_trimester = patients_trimester,
    pt = patients
  )
  
  trimester_hierarchy1 <- trimester_hierarchy1_list[["trimester_hierarchy1"]]
  trimester_lvl1_dups <- trimester_hierarchy1_list[["trimester_lvl1_dups"]]
  
  # Apply Hierarchy #2
  
  trimester_hierarchy2_list <- trimester_lvl1_dups %>%
    apply_trimester_hierarchy_2()
  
  trimester_lvl2_dups <- trimester_hierarchy2_list[["trimester_lvl2_dups"]]
  trimester_hierarchy2 <- trimester_hierarchy2_list[["trimester_hierarchy2"]]
  z3a_any <- trimester_hierarchy2_list[["z3a_any"]]
  
  # Apply Hierarchy #3
  
  trimester_hierarchy3_list <- apply_trimester_hierarchy_3(
    trimester_lvl2_dups = trimester_lvl2_dups,
    z3a_any = z3a_any
  )
  
  trimester_hierarchy3 <- trimester_hierarchy3_list[["trimester_hierarchy3"]]
  trimester_lvl3_dups <- trimester_hierarchy3_list[["trimester_lvl3_dups"]]
  
  # Apply Hierarchy #4
  
  trimester_hierarchy4_list <- apply_trimester_hierarchy_4(trimester_lvl3_dups)
  
  trimester_hierarchy4 <- trimester_hierarchy4_list[["trimester_hierarchy4"]]
  trimester_lvl4_dups <- trimester_hierarchy4_list[["trimester_lvl4_dups"]]
  
  # Apply Hierarchy #5
  
  trimester_hierarchy5_list <- apply_trimester_hierarchy_5(trimester_lvl4_dups, labor_delivery_codes)
  
  trimester_hierarchy5 <- trimester_hierarchy5_list[["trimester_hierarchy5"]]
  trimester_lvl5_dups <- trimester_hierarchy5_list[["trimester_lvl5_dups"]]
  
  # Apply Hierarchy #6
  
  trimester_hierarchy6_list <- apply_trimester_hierarchy_6(trimester_lvl5_dups)
  
  trimester_hierarchy6 <- trimester_hierarchy6_list[["trimester_hierarchy6"]]
  trimester_lvl6_dups <- trimester_hierarchy6_list[["trimester_lvl6_dups"]]
  
  # Apply Hierarchy #7
  
  trimester_hierarchy7_list <- apply_trimester_hierarchy_7(trimester_lvl6_dups)
  
  trimester_hierarchy7 <- trimester_hierarchy7_list[["trimester_hierarchy7"]]
  trimester_lvl7_dups <- trimester_hierarchy7_list[["trimester_lvl7_dups"]]
  
  # Apply Hierarchy #8
  
  trimester_hierarchy8_list <- trimester_lvl7_dups %>%
    apply_trimester_hierarchy_8()
  
  trimester_hierarchy8 <- trimester_hierarchy8_list[["trimester_hierarchy8"]]
  trimester_lvl8_dups <- trimester_hierarchy8_list[["trimester_lvl8_dups"]]
  
  # Apply Hierarchy #9
  
  trimester_hierarchy9_list <- apply_trimester_hierarchy_9(
    trimester_hierarchy1 = trimester_hierarchy1,
    trimester_hierarchy2 = trimester_hierarchy2,
    trimester_hierarchy3 = trimester_hierarchy3,
    trimester_hierarchy4 = trimester_hierarchy4,
    trimester_hierarchy5 = trimester_hierarchy5,
    trimester_hierarchy6 = trimester_hierarchy6,
    trimester_hierarchy7 = trimester_hierarchy7,
    trimester_hierarchy8 = trimester_hierarchy8,
    trimester_lvl8_dups = trimester_lvl8_dups
  )
  
  trimester_hierarchy9 <- trimester_hierarchy9_list[["trimester_hierarchy9"]]
  trimester_lvl9_dups <- trimester_hierarchy9_list[["trimester_lvl9_dups"]]
  trimester_hierarchy123456789 <- trimester_hierarchy9_list[["trimester_hierarchy123456789"]]
  
  # Apply Hierarchy #10
  
  trimester_hierarchy10_list <- apply_trimester_hierarchy_10(trimester_lvl9_dups)
  
  trimester_hierarchy10 <- trimester_hierarchy10_list[["trimester_hierarchy10"]]
  trimester_lvl10_dups <- trimester_hierarchy10_list[["trimester_lvl10_dups"]]
  
  # Apply Hierarchy #11
  
  trimester_hierarchy11_list <- apply_trimester_hierarchy_11(trimester_lvl10_dups)
  
  trimester_hierarchy11 <- trimester_hierarchy11_list[["trimester_hierarchy11"]]
  trimester_lvl11_dups <- trimester_hierarchy11_list[["trimester_lvl11_dups"]]
  
  # Note: the dataset will be used in 040*.sas and only keep the variables as needed.
  trimester_hierarchy_final <- bind_rows(
    trimester_hierarchy123456789,
    trimester_hierarchy10,
    trimester_hierarchy11,
    trimester_lvl11_dups
  ) %>%
    mutate(
      srv_year = year(svcdate),
      secondary_outcome = str_trim(secondary_outcome, side = "both"),
      secondary_outcome = replace_na(secondary_outcome, "") # replacing NAs with "" to get clean compare!
    ) %>%
    select(enrolid, svcdate, code, claims_file, description, final_trimester_code, secondary_outcome, srv_year, outcome, pdx, trimester_code_clean) %>%
    distinct() %>%
    as_tibble()

  # Garbage collection -------
  gc()
  
  return(trimester_hierarchy_final)
  
},
error = function(e) {
  message("The following errors are likely due to an insufficient record sample size. Consider broadening the age range of interest or extending the study duration.")
  stop(e)
})
}

# Trimester hierarchy helpers -------------

#' Apply Trimester Hierarchy #1
#' @description
#' Classify trimester codes into 4 tiers:  tier 1 = 1,2 or 3; tier 2 = 4 or 5; tier 3 = 9; tier 4 = 8;
#' Trimester code 1 is assigned if a trimester code 1 exists and trimester codes 2, 3, 5 do not exist on the same service day
#' Trimester code 2 is assigned if a trimester code 2 exists and trimester codes 1, 3 do not exist on the same service day
#' Trimester code 3 is assigned if a trimester code 3 exists and trimester codes 1, 2 & 4 do not exist on the same service day
#' Trimester code 4 is assigned if a trimester code 4 exists and trimester codes 1, 2, 3 & 5 do not exist on the same service day
#' Trimester code 5 is assigned if a trimester code 5 exists and trimester codes 1, 2, 3 & 4 do not exist on the same service day
#' Trimester code 9 is assigned if a trimester code 9 exists and trimester codes 1, 2, 3, 4 & 5 do not exist on the same service day
#'
#'
#' @param pt_trimester A data frame containing patient data with the format of the exported dataset extraction example dataset.
#' @param pt Same as pt_trimester but with dummy columns.
#'
#' @import dplyr
#' @import dtplyr
#' 
#' @return list
#' @noRd
apply_trimester_hierarchy_1 <- function(pt_trimester, pt) {
  pt_trimester2 <- pt_trimester %>%
    lazy_dt() %>%
    group_by(enrolid, svcdate) %>%
    summarise(
      trimester_1 = sum(trimester_1),
      trimester_2 = sum(trimester_2),
      trimester_3 = sum(trimester_3),
      trimester_4 = sum(trimester_4),
      trimester_5 = sum(trimester_5),
      trimester_8 = sum(trimester_8),
      trimester_9 = sum(trimester_9)
    ) %>%
    as_tibble()
  
  pt_trimester3 <- pt %>%
    lazy_dt() %>%
    left_join(pt_trimester2, by = c("enrolid", "svcdate")) %>%
    relocate(enrolid, dobyr, code, description, pdx, claims_file, svcdate, final_trimester_code, outcome, secondary_outcome, trimester_1, trimester_2, trimester_3, trimester_4, trimester_5, trimester_8, trimester_9) %>%
    mutate(
      tier = case_when(
        final_trimester_code %in% 1:3 ~ 1,
        final_trimester_code %in% 4:5 ~ 2,
        final_trimester_code %in% 9 ~ 3,
        final_trimester_code %in% 8 ~ 4,
        TRUE ~ NA_real_
      ),
      trimester_code_clean = case_when(
        trimester_1 > 0 &
          trimester_2 == 0 &
          trimester_3 == 0 &
          trimester_5 == 0 ~ 1,
        trimester_1 == 0 &
          trimester_2 > 0 &
          trimester_3 == 0 ~ 2,
        trimester_1 == 0 &
          trimester_2 == 0 &
          trimester_3 > 0 &
          trimester_4 == 0 ~ 3,
        trimester_1 == 0 &
          trimester_2 == 0 &
          trimester_3 == 0 &
          trimester_4 > 0 &
          trimester_5 == 0 ~ 4,
        trimester_1 == 0 &
          trimester_2 == 0 &
          trimester_3 == 0 &
          trimester_4 == 0 &
          trimester_5 > 0 ~ 5,
        trimester_1 == 0 &
          trimester_2 == 0 &
          trimester_3 == 0 &
          trimester_4 == 0 &
          trimester_5 == 0 &
          trimester_9 == 0 ~ 8,
        trimester_1 == 0 &
          trimester_2 == 0 &
          trimester_3 == 0 &
          trimester_4 == 0 &
          trimester_5 == 0 &
          trimester_9 > 0 ~ 9,
        TRUE ~ NA_real_
      )
    ) %>%
    as_tibble()
  
  trimester_hierarchy1 <- pt_trimester3 %>%
    drop_NAs_in_trimester_code_clean()
  
  trimester_lvl1_dups <- pt_trimester3 %>%
    keep_only_NAs_in_trimester_code_clean()
  
  return(list(
    trimester_hierarchy1 = trimester_hierarchy1,
    trimester_lvl1_dups = trimester_lvl1_dups
  ))
}

#' Apply Trimester Hierarchy #2
#' @description
#' All other codes that incur on the same service day as the Z3AXX, P073X & P072X codes, which specify the number of gestation weeks,
#' will be reassigned the trimester code of the Z3AXX/P073X/P072X  code.  If there are multiple Z3AXX/P073X/P072X codes occur on the same day,
#' the code with the latest weeks specified will be used. Since codes 'Z3A00' 'Z3A' 'Z3A0' 'Z3A01' 'Z3A1' 'Z3A2' 'Z3A3' 'Z3A4' do not specify
#' the number of gestation weeks, they will not be included in this hierarchy.
#'
#' @param trimester_lvl1_dups
#'
#' @import dplyr
#' @import dtplyr
#' @import assertthat
#' @import stringr
#' 
#' @return list
#' @noRd
apply_trimester_hierarchy_2 <- function(trimester_lvl1_dups) {
  # This check makes sure that z3a_p07_flag does not contain NAs
  assert_that(noNA(trimester_lvl1_dups$code))
  
  suppressWarnings( # In str_sub(code, 4L, 6L) %>% str_squish() %>% as.integer() : NAs introduced by coercion
  trimester_lvl1_dups_2 <- lazy_dt(trimester_lvl1_dups) %>%
    mutate(
      z3a_p07_flag = if_else(
        str_detect(code, "Z3A") &
          !(code %in% c("Z3A00", "Z3A")) |
          code %in% c(
            "P0739",
            "P0738",
            "P0737",
            "P0736",
            "P0735",
            "P0734",
            "P0733",
            "P0732",
            "P0731",
            "P0726",
            "P0725",
            "P0724",
            "P0723",
            "P0722",
            "P0721"
          ),
        1,
        0
      ),
      z3a = if_else(str_detect(code, "Z3A") &
                      !(code %in% c("Z3A00", "Z3A", "Z3A0", "Z3A01", "Z3A1", "Z3A2", "Z3A3", "Z3A4")),
                    1, 0
      ),
      z3a_weeks = if_else(
        z3a == 1,
        str_sub(code, 4L, 6L) %>%
          str_squish() %>%
          as.integer(),
        NA_integer_
      )
    ) %>%
    select(-trimester_code_clean) %>%
    as_tibble()
  )
  
  trimester_lvl1_dups_3 <- lazy_dt(trimester_lvl1_dups_2) %>%
    group_by(enrolid, svcdate) %>%
    summarise(z3a_p07_any = max(z3a_p07_flag)) %>%
    as_tibble()
  
  trimester_lvl1_dups_4 <- lazy_dt(trimester_lvl1_dups_2) %>%
    left_join(trimester_lvl1_dups_3,
              by = c("enrolid", "svcdate")
    ) %>%
    arrange(enrolid, svcdate, code) %>% # this step was missing earlier
    as_tibble()
  
  trimester_lvl1_dups_4_split <- trimester_lvl1_dups_4 %>%
    mutate(z3a_filter = if_else(z3a_p07_any == 1, 1, 0)) %>%
    select(-z3a, -z3a_weeks, -dobyr) %>%
    select(
      enrolid, svcdate, code, pdx, srv_year, claims_file, description, final_trimester_code, outcome, secondary_outcome,
      trimester_1, trimester_2, trimester_3, trimester_4, trimester_5, trimester_8, trimester_9, tier, z3a_p07_flag, z3a_p07_any, z3a_filter
    )
  
  trimester_lvl1_dups_5 <- trimester_lvl1_dups_4_split %>%
    filter(z3a_filter == 1) %>%
    arrange(desc(z3a_p07_flag), desc(final_trimester_code)) %>%
    arrange(tier) %>%
    arrange(enrolid, svcdate)
  
  trimester_lvl2_dups <- trimester_lvl1_dups_4_split %>%
    filter(z3a_filter == 0) %>%
    arrange(enrolid)
  
  trimester_lvl1_dups_6 <- trimester_lvl1_dups_5 %>%
    distinct(enrolid, svcdate, .keep_all = TRUE) %>%
    rename(trimester_code_clean = final_trimester_code) %>%
    select(enrolid, svcdate, trimester_code_clean)
  
  trimester_hierarchy2 <- trimester_lvl1_dups_5 %>%
    left_join(trimester_lvl1_dups_6,
              by = c("enrolid", "svcdate")
    )
  
  z3a_any <- trimester_lvl1_dups_2 %>%
    filter(z3a == 1) %>%
    select(enrolid, svcdate, z3a, z3a_weeks) %>%
    distinct() %>%
    arrange(enrolid, svcdate, z3a)
  
  return(list(
    trimester_lvl2_dups = trimester_lvl2_dups,
    trimester_hierarchy2 = trimester_hierarchy2,
    z3a_any = z3a_any
  ))
}

#' Apply Trimester Hierarchy #3
#' 
#' If patient has a Z3A code, use the weeks to calculate the correct trimester for those with conflicting before & after.
#'
#' @param trimester_lvl2_dups Refer to 
#' @param z3a_any
#'
#' @import dplyr
#' @import dtplyr
#' 
#' @return list
#' @noRd
apply_trimester_hierarchy_3 <- function(trimester_lvl2_dups, z3a_any) {
  trimester_lvl2_dups_2 <- trimester_lvl2_dups %>%
    left_join(z3a_any %>%
                rename(z3a_dt = svcdate) %>%
                select(enrolid, z3a_dt, z3a_weeks, z3a), by = "enrolid")
  # had to split trimester_lvl2_dups_3 into three datasets i.e. trimester_lvl2_dups_3_1/2/3 to cover all the cases in trimester_lvl2_dups_3 for diff_weeks
  # created Nsundays function to match output with SAS, in this function while subtracting two dates, date on R.H.S should be gt than L.H.S
  # in the following steps three separate datasets created to cover all the three cases i.e.+ve/-ve/NA
  
  trimester_lvl2_dups_3_1 <- trimester_lvl2_dups_2 %>%
    filter(!is.na(z3a_dt) & svcdate > z3a_dt) %>%
    mutate(diff_weeks = case_when(z3a == 1 ~ Nsundays(z3a_dt, svcdate)))
  
  trimester_lvl2_dups_3_2 <- trimester_lvl2_dups_2 %>%
    filter(!is.na(z3a_dt) & z3a_dt > svcdate) %>%
    #-1 is used to convert into -ve values as Nsundays fun will not work if RHS date value is less than LHS date value.It's a hack to get -ve values
    mutate(diff_weeks = case_when(z3a == 1 ~ -1 * (Nsundays(svcdate, z3a_dt))))
  
  trimester_lvl2_dups_3_3 <- trimester_lvl2_dups_2 %>%
    filter(is.na(z3a_dt)) %>%
    mutate(diff_weeks = as.Date(NA))
  
  trimester_lvl2_dups_3 <- rbind(trimester_lvl2_dups_3_1, trimester_lvl2_dups_3_2, trimester_lvl2_dups_3_3) %>%
    mutate(
      srv_preg_weeks = if_else(z3a == 1,
                               abs(z3a_weeks + diff_weeks),
                               NA_real_
      ),
      trimester_code_clean = case_when(
        diff_weeks >= -42 & diff_weeks <= 42 & srv_preg_weeks >= 0 & srv_preg_weeks <= 13 ~ 1,
        diff_weeks >= -42 & diff_weeks <= 42 & srv_preg_weeks >= 14 & srv_preg_weeks <= 27 ~ 2,
        diff_weeks >= -42 & diff_weeks <= 42 & srv_preg_weeks >= 28 & srv_preg_weeks <= 42 ~ 3,
        TRUE ~ NA_real_
      )
    ) %>%
    select(enrolid, svcdate, code, z3a_dt, z3a_weeks, z3a, diff_weeks, srv_preg_weeks, trimester_code_clean)
  
  trimester_lvl2_dups_4 <- trimester_lvl2_dups_3 %>%
    drop_NAs_in_trimester_code_clean() %>%
    select(enrolid, svcdate, trimester_code_clean) %>%
    distinct()
  
  trimester_lvl2_dups_5 <- trimester_lvl2_dups_4 %>%
    select(enrolid, svcdate) %>%
    group_by(enrolid, svcdate) %>%
    summarise(tri_cnt = n())
  
  # Note that we need nondup_flag as 1 only for cases where trimester_code_clean ne . and tri_cnt = 1.
  # We are doing this by assigning all cases as "1" for nondup & then filtering the above criteria in next step.
  # Step 1: Assign nondup_flag=1 for all record;
  
  trimester_lvl2_dups_6 <- trimester_lvl2_dups_3 %>%
    left_join(trimester_lvl2_dups_5, by = c("enrolid", "svcdate")) %>%
    mutate(nondup_flag = 1)
  
  # Step2: Filter the criteria where trimester code clean is not null & has count 1
  trimester_lvl2_dups_7 <- trimester_lvl2_dups_6 %>%
    filter(!(is.na(trimester_code_clean)) & tri_cnt == 1) %>%
    select(enrolid, svcdate, nondup_flag, trimester_code_clean) %>%
    distinct()
  
  trimester_lvl2_dups_8 <- trimester_lvl2_dups %>%
    left_join(trimester_lvl2_dups_7, by = c("enrolid", "svcdate")) %>%
    select(
      enrolid, svcdate, code, pdx, srv_year, claims_file, description, final_trimester_code,
      outcome, secondary_outcome, trimester_1, trimester_2, trimester_3, trimester_4, trimester_5,
      trimester_8, trimester_9, tier, z3a_p07_flag, z3a_p07_any, nondup_flag, trimester_code_clean
    ) %>%
    distinct()
  
  trimester_hierarchy3 <- lazy_dt(trimester_lvl2_dups_8) %>%
    filter(nondup_flag == 1) %>%
    as_tibble()
  
  trimester_lvl3_dups <- lazy_dt(trimester_lvl2_dups_8) %>%
    filter(is.na(nondup_flag)) %>%
    select(-nondup_flag, -trimester_code_clean) %>%
    as_tibble()
  
  return(list(
    trimester_hierarchy3 = trimester_hierarchy3,
    trimester_lvl3_dups = trimester_lvl3_dups
  ))
}

#' Apply Trimester Hierarchy #4
#' 
#' @description
#' Count the number of trimester codes group (i.e. #1s, #2s, etc) that are occurring on the same service day.
#' @details
#' If there are multiple codes for the same trimester and a few for a different trimester occurring on same day,
#' use codes for the trimester assignment that has the most instances.
#' Trimester code 1 is assigned if the # of trimester codes for 1 is greater than the
#' num of trimester codes for 2 & 3 and greater than or equal to the # trimester codes for 4, 5
#' Trimester code 2 is assigned if the # of trimester codes for 2 is greater than the
#' num of trimester codes for 1 & 3 and greater than or equal to the # trimester codes for 4, 5
#' Trimester code 3 is assigned if the # of trimester codes for 3 is greater than the
#' num of trimester codes for 1 & 2 and greater than or equal to the # trimester codes for 4, 5
#' Trimester code 4 is assigned if the # of trimester codes for 4 is greater than the
#' num of trimester codes for 5 and trimester codes 1, 2 & 3 do not exist on the same service day
#' Trimester code 5 is assigned if the # of trimester codes for 5 is greater than the
#' num of trimester codes for 4 and trimester codes 1, 2 & 3 do not exist on the same service day
#'
#' @param trimester_lvl3_dups
#'
#' @import dplyr
#' @import dtplyr
#' 
#' @return list
#' @noRd
apply_trimester_hierarchy_4 <- function(trimester_lvl3_dups) {
  trimester_lvl3_dups_clean <- lazy_dt(trimester_lvl3_dups) %>%
    mutate(trimester_code_clean = case_when(
      trimester_1 > trimester_2 & trimester_1 > trimester_3 & trimester_1 >= trimester_4 & trimester_1 >= trimester_5 ~ 1,
      trimester_2 > trimester_1 & trimester_2 > trimester_3 & trimester_2 >= trimester_4 & trimester_2 >= trimester_5 ~ 2,
      trimester_3 > trimester_1 & trimester_3 > trimester_2 & trimester_3 >= trimester_4 & trimester_3 >= trimester_5 ~ 3,
      trimester_1 == 0 & trimester_2 == 0 & trimester_3 == 0 & trimester_4 > trimester_5 ~ 4,
      trimester_1 == 0 & trimester_2 == 0 & trimester_3 == 0 & trimester_5 > trimester_4 ~ 5,
      TRUE ~ NA_real_
    )) %>%
    select(-z3a_p07_flag, -z3a_p07_any) %>%
    as_tibble()
  
  trimester_hierarchy4 <- trimester_lvl3_dups_clean %>%
    drop_NAs_in_trimester_code_clean()
  
  trimester_lvl4_dups <- trimester_lvl3_dups_clean %>%
    keep_only_NAs_in_trimester_code_clean()
  
  return(list(
    trimester_hierarchy4 = trimester_hierarchy4,
    trimester_lvl4_dups = trimester_lvl4_dups
  ))
}

#' Apply Trimester Hierarchy #5
#' @description
#' 
#' Any Pregnancy loss: Ectopic pregnancy, Induced abortion, Spontaneous abortion Stillbirth and Unclassified pregnancy loss
#'
#' Combinations:
#' 	If a spontaneous abortion code and a stillbirth code occur on the same day classify trimester coding as spontaneous abortion
#' 	If a spontaneous abortion code, a stillbirth code and an induced abortion code and/or an unclassified loss code occur on the same day classify trimester coding as spontaneous abortion
#' 	If an induced abortion code, a stillbirth code and no spontaneous abortion code occur on the same day classify trimester coding as induced abortion
#' 	If an induced abortion code, a stillbirth code, an unclassified loss code and no spontaneous abortion code occur on the same day classify trimester coding as induced abortion
#' 	If an Any Pregnancy loss code occurs on the same day as a labor or delivery code (L and D list) follow Any Pregnancy loss code trimester coding
#'
#' Of the above combinations, if multiple conflicting pregnancy loss trimester codes appear on the same service date,
#' select the highest trimester code and the lowest tier ranking (i.e.  tier 1 = 1,2 or 3; tier 2 = 4 or 5; tier 3 = 9; tier 4 = 8)
#'
#' @param trimester_lvl4_dups trimester_lvl4_dups
#' @param labor_delivery_codes labor_delivery_codes
#'
#' @import dplyr
#' @import dtplyr
#' 
#' @return list
#' @noRd
apply_trimester_hierarchy_5 <- function(trimester_lvl4_dups, labor_delivery_codes) {
  trimester_lvl4_dups_2 <- lazy_dt(trimester_lvl4_dups) %>%
    mutate(
      secondary_outcome_without_na = replace_na(secondary_outcome, " "),
      induced_abortion = if_else((outcome == "Induced abortion" | secondary_outcome_without_na == "Induced abortion"), 1, 0),
      spons_abortion = if_else(outcome == "Spontaneous abortion" | secondary_outcome_without_na == "Spontaneous abortion", 1, 0),
      ectopic_preg = if_else(outcome == "Ectopic pregnancy" | secondary_outcome_without_na == "Ectopic pregnancy", 1, 0),
      unclass_preg_loss = if_else(outcome == "Unclassified pregnancy loss" | secondary_outcome_without_na == "Unclassified pregnancy loss", 1, 0),
      stillbirth = if_else(outcome == "Stillbirth" | secondary_outcome_without_na == "Stillbirth", 1, 0)
    ) %>%
    select(-trimester_code_clean) %>%
    as_tibble()
  
  labor_delivery <- labor_delivery_codes %>%
    mutate(labor_delivery_flag = 1) %>%
    select(-description, -file) %>%
    as_tibble()
  
  trimester_lvl4_dups_3 <- trimester_lvl4_dups_2 %>%
    left_join(labor_delivery, by = "code") %>%
    mutate(labor_delivery_flag = if_else(is.na(labor_delivery_flag), 0, labor_delivery_flag))
  
  trimester_lvl4_dups_4 <- trimester_lvl4_dups_3 %>%
    group_by(enrolid, svcdate) %>%
    summarise(
      induced_abortion_any = max(induced_abortion, na.rm = TRUE),
      spons_abortion_any = max(spons_abortion, na.rm = TRUE),
      ectopic_preg_any = max(ectopic_preg, na.rm = TRUE),
      unclass_preg_loss_any = max(unclass_preg_loss, na.rm = TRUE),
      stillbirth_any = max(stillbirth, na.rm = TRUE),
      labor_delivery_any = max(labor_delivery_flag, na.rm = TRUE)
    ) %>%
    select(
      enrolid, svcdate, induced_abortion_any,
      spons_abortion_any, ectopic_preg_any, unclass_preg_loss_any, stillbirth_any, labor_delivery_any
    ) %>%
    distinct()
  
  trimester_lvl4_dups_5 <- trimester_lvl4_dups_3 %>%
    left_join(trimester_lvl4_dups_4, by = c("enrolid", "svcdate")) %>%
    mutate(split = if_else(((spons_abortion_any == 1 & stillbirth_any == 1 & ectopic_preg_any == 0) |
                              (induced_abortion_any == 1 & stillbirth_any == 1 & ectopic_preg_any == 0 & spons_abortion_any == 0) |
                              (spons_abortion_any == 1 & labor_delivery_any == 1) |
                              (induced_abortion_any == 1 & labor_delivery_any == 1) |
                              (ectopic_preg_any == 1 & labor_delivery_any == 1) |
                              (unclass_preg_loss_any == 1 & labor_delivery_any == 1)), 1, 0))
  
  trimester_lvl4_dups_6 <- lazy_dt(trimester_lvl4_dups_5) %>%
    filter(split == 1) %>%
    select(-split) %>%
    as_tibble()
  
  trimester_lvl5_dups <- lazy_dt(trimester_lvl4_dups_5) %>%
    filter(split == 0) %>%
    select(-c(split, secondary_outcome)) %>%
    rename(secondary_outcome = secondary_outcome_without_na) %>%
    as_tibble()
  
  trimester_lvl4_dups_7 <- lazy_dt(trimester_lvl4_dups_6) %>%
    mutate(
      tier = case_when(
        final_trimester_code %in% 1:3 ~ 1,
        final_trimester_code %in% 4:5 ~ 2,
        final_trimester_code == 9 ~ 3,
        final_trimester_code == 8 ~ 4
      ),
      spon_abortion_stillbirth = if_else(spons_abortion_any == 1 & stillbirth_any == 1 & ectopic_preg_any == 0, 1, 0),
      ind_abortion_stillbirth = if_else(induced_abortion_any == 1 & stillbirth_any == 1 & ectopic_preg_any == 0 & spons_abortion_any == 0, 1, 0)
    ) %>%
    as_tibble()
  
  losses_only <- trimester_lvl4_dups_7 %>%
    filter(induced_abortion == 1 | spons_abortion == 1 | ectopic_preg == 1 | unclass_preg_loss == 1 | stillbirth == 1) %>%
    arrange(desc(spon_abortion_stillbirth)) %>%
    arrange(desc(ind_abortion_stillbirth)) %>%
    arrange(desc(final_trimester_code)) %>%
    arrange(tier) %>%
    arrange(enrolid, svcdate)
  
  losses_only_2 <- losses_only %>%
    group_by(enrolid, svcdate) %>%
    slice_head(n = 1)
  
  trimester_hierarchy5 <- trimester_lvl4_dups_7 %>%
    left_join(losses_only_2 %>% select(enrolid, svcdate, final_trimester_code), by = c("enrolid", "svcdate")) %>%
    mutate(trimester_code_clean = final_trimester_code.y) %>%
    select(-c(final_trimester_code.y, secondary_outcome)) %>%
    rename(
      final_trimester_code = final_trimester_code.x,
      secondary_outcome = secondary_outcome_without_na
    )
  
  return(list(
    trimester_hierarchy5 = trimester_hierarchy5,
    trimester_lvl5_dups = trimester_lvl5_dups
  ))
}


#' Apply Trimester Hierarchy #6
#'
#' Primary and Secondary diagnosis will be identified. If the trimester assignment for the code in the secondary position
#' does not match code in primary position, use primary trimester assignment. If multiple still exist, count the number of
#' primary dx trimester codes group (i.e. #1s, #2s, etc) that are occurring on the same service day in repeat assignment
#' logic used in Hierarchy #1 using count of primary only
#'
#'
#' @param trimester_lvl5_dups
#'
#' @import dplyr
#' @import dtplyr
#' 
#' @return list
#' @noRd
apply_trimester_hierarchy_6 <- function(trimester_lvl5_dups) {
  trimester_lvl5_dups_2 <- lazy_dt(trimester_lvl5_dups) %>%
    mutate(
      pdx_trimester_1 = if_else(pdx == 1, if_else(final_trimester_code == 1, 1, 0), NA_real_),
      pdx_trimester_2 = if_else(pdx == 1, if_else(final_trimester_code == 2, 1, 0), NA_real_),
      pdx_trimester_3 = if_else(pdx == 1, if_else(final_trimester_code == 3, 1, 0), NA_real_),
      pdx_trimester_4 = if_else(pdx == 1, if_else(final_trimester_code == 4, 1, 0), NA_real_),
      pdx_trimester_5 = if_else(pdx == 1, if_else(final_trimester_code == 5, 1, 0), NA_real_),
      pdx_trimester_8 = if_else(pdx == 1, if_else(final_trimester_code == 8, 1, 0), NA_real_),
      pdx_trimester_9 = if_else(pdx == 1, if_else(final_trimester_code == 9, 1, 0), NA_real_)
    ) %>%
    as_tibble()
  
  trimester_lvl5_dups_3_1 <- lazy_dt(trimester_lvl5_dups_2) %>%
    filter(pdx == 1) %>%
    group_by(enrolid, svcdate) %>%
    summarise(
      pdx_trimester_1 = sum(pdx_trimester_1),
      pdx_trimester_2 = sum(pdx_trimester_2),
      pdx_trimester_3 = sum(pdx_trimester_3),
      pdx_trimester_4 = sum(pdx_trimester_4),
      pdx_trimester_5 = sum(pdx_trimester_5),
      pdx_trimester_8 = sum(pdx_trimester_8),
      pdx_trimester_9 = sum(pdx_trimester_9)
    ) %>%
    as_tibble()
  
  trimester_lvl5_dups_3_2 <- trimester_lvl5_dups_2 %>%
    left_join(trimester_lvl5_dups_3_1, by = c("enrolid" = "enrolid", "svcdate" = "svcdate"), keep = TRUE) %>%
    filter(is.na(enrolid.y)) %>%
    group_by(enrolid.x, svcdate.x) %>%
    summarise(
      pdx_trimester_1 = sum(pdx_trimester_1.x),
      pdx_trimester_2 = sum(pdx_trimester_2.x),
      pdx_trimester_3 = sum(pdx_trimester_3.x),
      pdx_trimester_4 = sum(pdx_trimester_4.x),
      pdx_trimester_5 = sum(pdx_trimester_5.x),
      pdx_trimester_8 = sum(pdx_trimester_8.x),
      pdx_trimester_9 = sum(pdx_trimester_9.x)
    ) %>%
    rename(enrolid = enrolid.x, svcdate = svcdate.x) %>%
    select(
      enrolid, svcdate, pdx_trimester_1, pdx_trimester_2,
      pdx_trimester_3, pdx_trimester_4, pdx_trimester_5, pdx_trimester_8, pdx_trimester_9
    )
  
  # append datasets
  # bind_rows throws errors because of differences in types of variables in datasets, hence used rbind, with rbind type of variables don't matter!
  trimester_lvl5_dups_3 <- rbind(trimester_lvl5_dups_3_1, trimester_lvl5_dups_3_2)
  
  trimester_lvl5_dups_4 <- lazy_dt(trimester_lvl5_dups) %>%
    left_join(lazy_dt(trimester_lvl5_dups_3), by = c("enrolid", "svcdate")) %>%
    as_tibble()
  
  trimester_lvl5_dups_4_clean <- trimester_lvl5_dups_4 %>%
    mutate(trimester_code_clean = case_when(
      pdx_trimester_1 > 0 & pdx_trimester_2 == 0 & pdx_trimester_3 == 0 & pdx_trimester_5 == 0 ~ 1L,
      pdx_trimester_1 == 0 & pdx_trimester_2 > 0 & pdx_trimester_3 == 0 ~ 2L,
      pdx_trimester_1 == 0 & pdx_trimester_2 == 0 & pdx_trimester_3 > 0 & pdx_trimester_4 == 0 ~ 3L,
      pdx_trimester_1 == 0 & pdx_trimester_2 == 0 & pdx_trimester_3 == 0 & pdx_trimester_4 > 0 & pdx_trimester_5 == 0 ~ 4L,
      pdx_trimester_1 == 0 & pdx_trimester_2 == 0 & pdx_trimester_3 == 0 & pdx_trimester_4 == 0 & pdx_trimester_5 > 0 ~ 5L,
      TRUE ~ NA_integer_
    )) %>%
    select(-c(pdx_trimester_1, pdx_trimester_2, pdx_trimester_3, pdx_trimester_4, pdx_trimester_5, pdx_trimester_8, pdx_trimester_9)) %>%
    as_tibble()
  
  trimester_hierarchy6 <- trimester_lvl5_dups_4_clean %>%
    drop_NAs_in_trimester_code_clean()
  trimester_lvl6_dups <- trimester_lvl5_dups_4_clean %>%
    keep_only_NAs_in_trimester_code_clean()
  
  return(list(
    trimester_hierarchy6 = trimester_hierarchy6,
    trimester_lvl6_dups = trimester_lvl6_dups
  ))
}
#' Apply Trimester Hierarchy #7
#' 
#' Instances where there is a trimester code of 1, outcome = Delivery and no outcomes of Induced abortion
#' or Spontaneous abortion, then the trimester code will be reassigned a 5*/
#'
#' @param trimester_lvl6_dups
#'
#' @import dplyr
#' @import dtplyr
#' 
#' @return list
#' @noRd
apply_trimester_hierarchy_7 <- function(trimester_lvl6_dups) {
  trimester_lvl6_dups_2 <- lazy_dt(trimester_lvl6_dups) %>%
    mutate(
      delivery = if_else(outcome == "Delivery" | secondary_outcome == "Delivery", 1, 0),
      abortion = if_else(outcome %in% c("Induced abortion", "Spontaneous abortion") |
                           secondary_outcome %in% c("Induced abortion", "Spontaneous abortion"),
                         1, 0
      )
    ) %>%
    select(enrolid, svcdate, delivery, abortion) %>%
    as_tibble()
  
  trimester_lvl6_dups_3 <- lazy_dt(trimester_lvl6_dups_2) %>%
    group_by(enrolid, svcdate) %>%
    summarise(
      delivery = sum(delivery),
      abortion = sum(abortion)
    ) %>%
    as_tibble()
  
  trimester_lvl6_dups_5 <- lazy_dt(trimester_lvl6_dups) %>%
    left_join(lazy_dt(trimester_lvl6_dups_3), by = c("enrolid", "svcdate")) %>%
    select(-trimester_code_clean) %>%
    as_tibble()
  
  trimester_lvl6_dups_5_1 <- trimester_lvl6_dups_5 %>%
    mutate(trimester_code_clean = case_when(
      delivery >= 1 & trimester_1 >= 1 & abortion == 0 ~ 5,
      TRUE ~ 0
    ))
  
  trimester_hierarchy7 <- trimester_lvl6_dups_5_1 %>%
    filter(trimester_code_clean == 5) %>%
    select(-c(delivery, abortion))
  
  trimester_lvl7_dups <- trimester_lvl6_dups_5_1 %>%
    filter(trimester_code_clean == 0) %>%
    select(-c(delivery, abortion, trimester_code_clean))
  
  return(list(
    trimester_hierarchy7 = trimester_hierarchy7,
    trimester_lvl7_dups = trimester_lvl7_dups
  ))
}

#' Apply Trimester Hierarchy #8
#' 
#' Claims data files Inpatient and Outpatient will be identified. If the trimester assignment for the code
#' on the outpatient file does not match code on the inpatient file, use inpatient trimester assignment.
#' If multiple still exist, count the number of inpatient trimester codes group (i.e. #1s, #2s, etc) that are
#' occurring on the same service day in repeat assignment logic used in Hierarchy #1 using count of inpatient only
#' @param .data
#'
#' @import dplyr
#' @import dtplyr
#' @import assertthat
#' 
#' @return list
#' @noRd
apply_trimester_hierarchy_8 <- function(trimester_lvl7_dups) {
  assertthat::noNA(trimester_lvl7_dups$claims_file)
  # trimester_lvl7_dups <- lazy_dt(trimester_lvl7_dups)
  
  trimester_lvl7_dups_3 <- trimester_lvl7_dups %>%
    mutate(
      outpatient = if_else(claims_file == "O", 1, 0),
      inpatient = if_else(claims_file == "I", 1, 0)
    ) %>%
    group_by(enrolid, svcdate) %>%
    summarise(
      outpatient = max(outpatient),
      inpatient = max(inpatient)
    )
  
  trimester_lvl7_dups_4 <- trimester_lvl7_dups %>%
    left_join(trimester_lvl7_dups_3, by = c("enrolid", "svcdate")) %>%
    as_tibble()
  
  trimester_lvl7_dups_6 <- lazy_dt(trimester_lvl7_dups_4) %>%
    mutate(
      ip_trimester_1 = if_else(final_trimester_code == 1 & claims_file == "I", 1, 0),
      ip_trimester_2 = if_else(final_trimester_code == 2 & claims_file == "I", 1, 0),
      ip_trimester_3 = if_else(final_trimester_code == 3 & claims_file == "I", 1, 0),
      ip_trimester_4 = if_else(final_trimester_code == 4 & claims_file == "I", 1, 0),
      ip_trimester_5 = if_else(final_trimester_code == 5 & claims_file == "I", 1, 0),
      ip_trimester_8 = if_else(final_trimester_code == 8 & claims_file == "I", 1, 0),
      ip_trimester_9 = if_else(final_trimester_code == 9 & claims_file == "I", 1, 0)
    ) %>%
    group_by(enrolid, svcdate) %>%
    summarise(
      ip_trimester_1 = sum(ip_trimester_1),
      ip_trimester_2 = sum(ip_trimester_2),
      ip_trimester_3 = sum(ip_trimester_3),
      ip_trimester_4 = sum(ip_trimester_4),
      ip_trimester_5 = sum(ip_trimester_5),
      ip_trimester_8 = sum(ip_trimester_8),
      ip_trimester_9 = sum(ip_trimester_9)
    ) %>%
    as_tibble()
  
  trimester_lvl7_dups_7 <- lazy_dt(trimester_lvl7_dups_4) %>%
    left_join(lazy_dt(trimester_lvl7_dups_6), by = c("enrolid", "svcdate")) %>%
    as_tibble()
  
  trimester_lvl7_dups_7_clean <- trimester_lvl7_dups_7 %>%
    mutate(trimester_code_clean = case_when(
      outpatient == 1 & inpatient == 1 & ip_trimester_1 > 0 & ip_trimester_2 == 0 & ip_trimester_3 == 0 & ip_trimester_5 == 0 ~ 1,
      outpatient == 1 & inpatient == 1 & ip_trimester_1 == 0 & ip_trimester_2 > 0 & ip_trimester_3 == 0 ~ 2,
      outpatient == 1 & inpatient == 1 & ip_trimester_1 == 0 & ip_trimester_2 == 0 & ip_trimester_3 > 0 & ip_trimester_4 == 0 ~ 3,
      outpatient == 1 & inpatient == 1 & ip_trimester_1 == 0 & ip_trimester_2 == 0 & ip_trimester_3 == 0 & ip_trimester_4 > 0 & ip_trimester_5 == 0 ~ 4,
      outpatient == 1 & inpatient == 1 & ip_trimester_1 == 0 & ip_trimester_2 == 0 & ip_trimester_3 == 0 & ip_trimester_4 == 0 & ip_trimester_5 > 0 ~ 5,
      TRUE ~ NA_real_
    )) %>%
    as_tibble()
  
  trimester_hierarchy8 <- trimester_lvl7_dups_7_clean %>%
    drop_NAs_in_trimester_code_clean()
  trimester_lvl8_dups <- trimester_lvl7_dups_7_clean %>%
    keep_only_NAs_in_trimester_code_clean()
  
  return(list(
    trimester_hierarchy8 = trimester_hierarchy8,
    trimester_lvl8_dups = trimester_lvl8_dups
  ))
}

#' Apply Trimester Hierarchy #9
#' 
#' Of remaining service day claims with conflicting trimester assignment, if additional claims exist for these
#' patients within 60 days, ignore and do not include them to assign the trimester*/
#'
#' @param trimester_hierarchy1 trimester_hierarchy1
#' @param trimester_hierarchy2 trimester_hierarchy2
#' @param trimester_hierarchy3 trimester_hierarchy3
#' @param trimester_hierarchy4 trimester_hierarchy4
#' @param trimester_hierarchy5 trimester_hierarchy5
#' @param trimester_hierarchy6 trimester_hierarchy6
#' @param trimester_hierarchy7 trimester_hierarchy7
#' @param trimester_hierarchy8 trimester_hierarchy8
#' @param trimester_lvl8_dups trimester_lvl8_dups
#'
#' @import dplyr
#' @import dtplyr
#' 
#' @return list
#' @noRd
apply_trimester_hierarchy_9 <- function(trimester_hierarchy1,
                                        trimester_hierarchy2,
                                        trimester_hierarchy3,
                                        trimester_hierarchy4,
                                        trimester_hierarchy5,
                                        trimester_hierarchy6,
                                        trimester_hierarchy7,
                                        trimester_hierarchy8,
                                        trimester_lvl8_dups) {
  nodups_claims <- bind_rows(
    trimester_hierarchy1,
    trimester_hierarchy2,
    trimester_hierarchy3,
    trimester_hierarchy4,
    trimester_hierarchy5,
    trimester_hierarchy6,
    trimester_hierarchy7,
    trimester_hierarchy8
  )
  
  trimester_lvl8_dups_data_test <- lazy_dt(trimester_lvl8_dups) %>%
    select(enrolid, svcdate) %>%
    distinct() %>%
    left_join(lazy_dt(nodups_claims), by = "enrolid") %>%
    rename(
      dup_svcdate = svcdate.x,
      svcdate = svcdate.y,
      dup_record = enrolid
    ) %>%
    as_tibble()
  
  trimester_lvl8_dups_data_test2 <- trimester_lvl8_dups_data_test %>%
    mutate(days_diff = abs(as.numeric(difftime(dup_svcdate, svcdate, units = "days")))) %>%
    select(dup_record, dup_svcdate, days_diff)
  
  trimester_lvl8_dups_data_test3 <- trimester_lvl8_dups_data_test2 %>%
    group_by(dup_record, dup_svcdate) %>%
    summarise(min_dt_to_dup_service = min(days_diff))
  
  trimester_lvl8_dups_data_test4 <- lazy_dt(trimester_lvl8_dups) %>%
    left_join(trimester_lvl8_dups_data_test3 %>%
                select(dup_record, dup_svcdate, min_dt_to_dup_service), by = c("enrolid" = "dup_record", "svcdate" = "dup_svcdate")) %>%
    as_tibble()
  
  trimester_lvl8_dups_data_test4_filter <- lazy_dt(trimester_lvl8_dups_data_test4) %>%
    mutate(
      final_trimester_code = if_else(min_dt_to_dup_service >= 1 & min_dt_to_dup_service <= 60 & !is.na(min_dt_to_dup_service), NA_real_, final_trimester_code),
      filter = if_else(min_dt_to_dup_service >= 1 & min_dt_to_dup_service <= 60 & !is.na(min_dt_to_dup_service), 1, 0)
    ) %>%
    as_tibble() %>%
    group_split(filter, .keep = FALSE)
  
  trimester_hierarchy9 <- trimester_lvl8_dups_data_test4_filter[[2]]
  trimester_lvl9_dups <- trimester_lvl8_dups_data_test4_filter[[1]] %>% select(-trimester_code_clean)
  
  trimester_hierarchy123456789 <- bind_rows(
    nodups_claims,
    trimester_hierarchy9
  ) %>%
    arrange(enrolid)
  
  return(list(
    trimester_hierarchy9 = trimester_hierarchy9,
    trimester_lvl9_dups = trimester_lvl9_dups,
    trimester_hierarchy123456789 = trimester_hierarchy123456789
  ))
}

#' Apply Trimester Hierarchy #10
#'
#' If patients have an ultrasound CPT code of ('76801' '76802') for a first trimester, conflicting codes that
#' incur on the same service day will be reassigned the CPT trimester code of 1
#'
#' @param trimester_lvl9_dups
#'
#' @import dplyr
#' @import dtplyr
#' 
#' @return list
#' @noRd
apply_trimester_hierarchy_10 <- function(trimester_lvl9_dups) {
  trimester_lvl9_dups_2 <- trimester_lvl9_dups %>%
    mutate(ultrasound_cpt = if_else(code %in% c("76801", "76802"), 1, 0))
  
  trimester_lvl9_dups_3 <- trimester_lvl9_dups_2 %>%
    group_by(enrolid, svcdate, final_trimester_code) %>%
    summarise(ultrasound_cpt = max(ultrasound_cpt)) %>%
    filter(ultrasound_cpt == 1) %>%
    rename(trimester_code_clean = final_trimester_code)
  
  trimester_lvl9_dups_4 <- trimester_lvl9_dups_2 %>%
    left_join(trimester_lvl9_dups_3, by = c("enrolid", "svcdate")) %>%
    select(-ultrasound_cpt.y) %>%
    rename(ultrasound_cpt = ultrasound_cpt.x)
  
  trimester_hierarchy10 <- trimester_lvl9_dups_4 %>%
    drop_NAs_in_trimester_code_clean()
  
  trimester_lvl10_dups <- trimester_lvl9_dups_4 %>%
    keep_only_NAs_in_trimester_code_clean()
  
  # trimester_lvl10_dups2 <- trimester_lvl10_dups %>%
  #   select(enrolid, svcdate, trimester_1, trimester_2, trimester_3, trimester_4, trimester_5, trimester_8) %>%
  #   distinct()
  
  return(list(
    trimester_hierarchy10 = trimester_hierarchy10,
    trimester_lvl10_dups = trimester_lvl10_dups
  ))
}

#' Apply Trimester Hierarchy #11
#' 
#' Reassign the trimesters based on the following scenarios:
#' Trimester code 2 is assigned if a trimester code 1, 2 & 5 exists and trimester codes 3 & 4 do not exist on the same service day
#' Trimester code 3 is assigned if a trimester code 3 & 8 exists and trimester code 4 do not exist on the same service day
#' Trimester code 3 is assigned if a trimester code 1, 3 & 5 exists and trimester code 4 do not exist on the same service day
#' Trimester code 4 is assigned if a trimester code 1 & 2 exists and trimester codes 3 & 5 do not exist on the same service day
#' Trimester code 5 is assigned if a trimester code 2 & 3 exists and trimester codes 1 & 8 do not exist on the same service day
#' Trimester code 5 is assigned if a trimester code 3, 4 & 5 exists and trimester codes 1 & 2 do not exist on the same service day
#'
#' Of remaining service day claims with conflicting trimester assignment, ignore and do not include them to assign the trimester
#'
#' @param trimester_lvl10_dups
#'
#' @import dplyr
#' 
#' @return list
#' @noRd
apply_trimester_hierarchy_11 <- function(trimester_lvl10_dups) {
  trimester_lvl10_dups_clean <- trimester_lvl10_dups %>%
    mutate(trimester_code_clean = case_when(
      trimester_1 == 0 & trimester_2 >= 1 & trimester_3 >= 1 & trimester_8 == 0 ~ 5,
      trimester_1 == 0 & trimester_2 == 0 & trimester_3 >= 1 & trimester_4 >= 1 & trimester_5 >= 1 ~ 5,
      trimester_1 >= 1 & trimester_2 >= 1 & trimester_3 == 0 & trimester_5 == 0 ~ 4,
      trimester_1 >= 1 & trimester_2 >= 1 & trimester_3 == 0 & trimester_4 == 0 & trimester_5 >= 1 ~ 2,
      trimester_3 >= 1 & trimester_4 == 0 & trimester_8 >= 1 ~ 3,
      trimester_1 >= 1 & trimester_3 >= 1 & trimester_4 == 0 & trimester_5 >= 1 ~ 3,
      TRUE ~ NA_real_
    ))
  
  trimester_hierarchy11 <- trimester_lvl10_dups_clean %>%
    drop_NAs_in_trimester_code_clean()
  trimester_lvl11_dups <- trimester_lvl10_dups_clean %>%
    keep_only_NAs_in_trimester_code_clean()
  
  list(
    trimester_hierarchy11 = trimester_hierarchy11,
    trimester_lvl11_dups = trimester_lvl11_dups
  )
}