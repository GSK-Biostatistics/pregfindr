% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils.R
\name{write_to_impala_then_read_lazy_table}
\alias{write_to_impala_then_read_lazy_table}
\title{Save table to Impala and return remote table}
\usage{
write_to_impala_then_read_lazy_table(
  tablename,
  object_to_save,
  connection = NULL,
  schema = NULL,
  overwrite = TRUE
)
}
\arguments{
\item{tablename}{string}

\item{object_to_save}{table}

\item{connection}{Database connection}

\item{schema}{Database schema}

\item{overwrite}{logic, TRUE overwrites table in database, FALSE simply reads}
}
\value{
remote table
}
\description{
Save table to Impala and return remote table
}
