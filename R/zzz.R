.onLoad <- function(libname, pkgname) {
  options(dplyr.summarise.inform = FALSE,
          .datatable.aware=TRUE)
  }
.onUnload <- function(libname, pkgname) {
  options(dplyr.summarise.inform = TRUE)
}