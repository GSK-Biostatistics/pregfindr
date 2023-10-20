#' ICD-9 Diagnosis Pregnancy Codes
#'
#' Pregnancy code list for ICD9
#'
#' @format A data frame with 1,927 rows and 6 columns:
#' \describe{
#'   \item{icd9cm}{ICD-9-CM code as a character string}
#'   \item{icd9cm_ihd}{ICD-9-CM code with a period as a character string}
#'   \item{description}{Description of the ICD-9-CM code}
#'   \item{outcome}{Primary outcome related to the ICD-9-CM code}
#'   \item{secondary_outcome}{Secondary outcome related to the ICD-9-CM code}
#'   \item{final_trimester_code}{Numeric code representing the final trimester}
#' }
#' @source Created internally within GSK
"icd9_diagnosis_pregnancy_codes"

#' ICD-10 Diagnosis Pregnancy Codes
#'
#' Pregnancy code list for ICD10
#'
#' @format A data frame with 3,132 rows and 6 columns:
#' \describe{
#'   \item{icd10cm}{ICD-10-CM code as a character string}
#'   \item{icd10cm_ihd}{ICD-10-CM code with a period as a character string}
#'   \item{description}{Description of the ICD-10-CM code}
#'   \item{outcome}{Primary pregnancy outcome related to the ICD-10-CM code}
#'   \item{secondary_outcome}{Secondary pregnancy outcome related to the ICD-10-CM code}
#'   \item{final_trimester_code}{Numeric code representing the final trimester}
#' }
#' @source Created internally within GSK
"icd10_diagnosis_pregnancy_codes"

#' ICD-9 Procedure Pregnancy Codes
#'
#' Pregnancy code list for ICD9 Procedure
#'
#' @format A data frame with 89 rows and 5 columns:
#' \describe{
#'   \item{icd9_proc}{ICD-9 Procedure code as a character string}
#'   \item{icd9_proc_ihd}{ICD-9 Procedure code with a period as a character string}
#'   \item{description}{Description of the ICD-9 Procedure code}
#'   \item{outcome}{Primary pregnancy outcome related to the ICD-9 Procedure code}
#'   \item{final_trimester_code}{Numeric code representing the final trimester}
#' }
#' @source Created internally within GSK
"icd9_procedure_pregnancy_codes"

#' ICD-10 Procedure Pregnancy Codes
#'
#' Pregnancy code list for ICD10 Procedure
#'
#' @format A data frame with 416 rows and 4 columns:
#' \describe{
#'   \item{icd10_proc}{ICD-10 Procedure code as a character string}
#'   \item{description}{Description of the ICD-10 Procedure code}
#'   \item{outcome}{Primary pregnancy outcome related to the ICD-10 Procedure code}
#'   \item{final_trimester_code}{Numeric code representing the final trimester}
#' }
#' @source Created internally within GSK
"icd10_procedure_pregnancy_codes"

#' CPT Pregnancy Codes
#'
#' Pregnancy code list for CPT
#'
#' @format A data frame with 181 rows and 6 columns:
#' \describe{
#'   \item{cpt_char}{CPT code as a character string}
#'   \item{cpt}{CPT code without leading zeros as a character string}
#'   \item{cpt_desc}{Description of the CPT code}
#'   \item{outcome}{Primary pregnancy outcome related to the CPT code}
#'   \item{secondary_outcome}{Secondary pregnancy outcome related to the CPT code}
#'   \item{final_trimester_code}{Numeric code representing the final trimester}
#' }
#' @source Created internally within GSK
"cpt_pregnancy_codes"

#' Labour and Delivery Codes
#'
#' Pregnancy code list for Labor and Delivery
#'
#' @format A data frame with 1,014 rows and 3 columns:
#' \describe{
#'   \item{code}{Code as a character string}
#'   \item{description}{Description of the code}
#'   \item{file}{Code type as a character string (i.e. "ICD10DX","ICD10PC","ICD9DX" ,"ICD9PC", and "CPT")}
#' }
#' @source Created internally within GSK
"labor_delivery_codes"

#' Extraction Example
#'
#' Example dataset showing what the results of the extraction step should look like.
#' 
#' @format A data frame with 14 rows and 11 columns:
#' \describe{
#'   \item{enrolid}{Enrolment ID as a double}
#'   \item{dobyr}{Year of birth as a double}
#'   \item{code}{Medical code as a character string}
#'   \item{description}{Description of the medical code}
#'   \item{pdx}{Primary diagnosis indicator as a double}
#'   \item{claims_file}{Claims file type as a character string}
#'   \item{svcdate}{Service date as a character string}
#'   \item{final_trimester_code}{Numeric code representing the final trimester as a double}
#'   \item{outcome}{Primary outcome related to the medical code}
#'   \item{secondary_outcome}{Secondary outcome related to the medical code as a logical}
#'   \item{srv_year}{Service year as a double}
#' }
"extraction_example"