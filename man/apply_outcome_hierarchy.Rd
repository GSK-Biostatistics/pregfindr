% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/apply_outcome_hierarchy.R
\name{apply_outcome_hierarchy}
\alias{apply_outcome_hierarchy}
\title{Apply Outcome Hierarchy to Patients}
\usage{
apply_outcome_hierarchy(trimester_hierarchy_final)
}
\arguments{
\item{trimester_hierarchy_final}{A data frame containing patient data with applied trimester hierarchy.}
}
\value{
A data frame with the applied outcome hierarchy logic, containing distinct records sorted by enrolid.
}
\description{
This function applies a series of outcome hierarchies to patient trimester data.
It combines outcomes for patients, adds outcome flags, and applies five levels of outcome hierarchy logic.
The final outcome hierarchy is returned as a data frame with distinct records, sorted by enrolid.
}
\examples{
\dontrun{
trimester_hierarchy_final <- apply_trimester_hierarchy(patients)
outcome_hierarchy_final <- apply_outcome_hierarchy(trimester_hierarchy_final)
}
}
