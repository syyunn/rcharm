# Title     : ipak package installer
# Objective : Install all packages at once
# Reference : https://gist.github.com/stevenworthington/3178163
# Created by: suyeol
# Created on: 2021/06/16

ipak <- function(pkg){
    new.pkg <- pkg[!(pkg %in% installed.packages()[, "Package"])]
    if (length(new.pkg))
        install.packages(new.pkg, dependencies = TRUE)
    sapply(pkg, require, character.only = TRUE)
}

# usage
packages <- c("dplyr", "dbplyr", "tidyr", "purrr", "zoo", "RMySQL")
ipak(packages)
