% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/data.R
\docType{data}
\name{cpt_pregnancy_codes}
\alias{cpt_pregnancy_codes}
\title{CPT Pregnancy Codes}
\format{
A data frame with 181 rows and 6 columns:
\describe{
\item{cpt_char}{CPT code as a character string}
\item{cpt}{CPT code without leading zeros as a character string}
\item{cpt_desc}{Description of the CPT code}
\item{outcome}{Primary pregnancy outcome related to the CPT code}
\item{secondary_outcome}{Secondary pregnancy outcome related to the CPT code}
\item{final_trimester_code}{Numeric code representing the final trimester}
}
}
\source{
Created internally within GSK
}
\usage{
cpt_pregnancy_codes
}
\description{
Pregnancy code list for CPT
}
\keyword{datasets}
