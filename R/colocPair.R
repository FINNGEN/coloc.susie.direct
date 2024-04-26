#!/usr/bin/env Rscript

#######################################################
# coloc pairs
# This function depends on the google cloud tools, to save the running resource
# Author: Zhili<zhilizheng@outlook.com>
#######################################################

require(data.table)
require(stringi)

args = commandArgs(TRUE)
# region information for finemap set 1
info1 = args[1]
# region information for finemap set 2
info2 = args[2]
# output filename
out = args[3]

dt1 = fread(info1)
dt2 = fread(info2)

dt1[, CHR:=stri_split_fixed(region, ":", simplify=TRUE)[, 1]]
dt1[, pos:=stri_split_fixed(region, ":", simplify=TRUE)[, 2]]
dt1[, start:=as.numeric(stri_split_fixed(pos, "-", simplify=TRUE)[, 1])]
dt1[, end:=as.numeric(stri_split_fixed(pos, "-", simplify=TRUE)[, 2])]

dt2[, CHR:=stri_split_fixed(region, ":", simplify=TRUE)[, 1]]
dt2[, pos:=stri_split_fixed(region, ":", simplify=TRUE)[, 2]]
dt2[, start2:=as.numeric(stri_split_fixed(pos, "-", simplify=TRUE)[, 1])]
dt2[, end2:=as.numeric(stri_split_fixed(pos, "-", simplify=TRUE)[, 2])]


dt3 = dt1[dt2, .(URL, trait, region, i.URL, i.trait, i.region), on=.(CHR, start <= end2, end >= start2), nomatch=0]


setnames(dt3, c("URL", "i.URL"), c("URL1", "URL2"))
setnames(dt3, c("trait", "i.trait"), c("trait1", "trait2"))
setnames(dt3, c("region", "i.region"), c("region1", "region2"))

fwrite(dt3[order(trait1, region1, trait2, region2)], file=out, sep="\t", col.names=F, na=NA)

message("Done")
