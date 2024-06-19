#!/usr/bin/env Rscript

#######################################################
# merge coloc among different dataset
# Author: Zhili<zhilizheng@outlook.com>
#######################################################


require(data.table)
require(stringi)

args = commandArgs(TRUE)

fileList = args[1]
h4Thresh = as.numeric(args[2])
cs_log10bf_thresh1 = as.numeric(args[3])
cs_log10bf_thresh2 = as.numeric(args[4])


files.all = readLines(fileList)
files.val = files.all[grepl("gz$", files.all)]

dts = list()
idx = 1
n = length(files.val)
nTotal = 0
for(file1 in files.val){
    idx = idx + 1
    message(idx, "/", n, ": ", file1)
    dt = fread(file1)
    name2 = gsub(".sum.tsv.gz", "", file1)
    name_sep = stri_split_fixed(name2, "-----", simplify=TRUE)

    dt[, colocRes:=file1]
    dt[, dataset1:=name_sep[1]]
    dt[, dataset2:=name_sep[2]]
    nTotal = nTotal + nrow(dt)
    message(" Current: ", nrow(dt), " rows, total: ", nTotal)

    dt.val = dt[PP.H4.abf >= h4Thresh]

    dts[[file1]] = dt.val
}

dt.sig = rbindlist(dts)
rm(dts)

dt1 = dt.sig[cs1==1 & cs2==1 & cs1_log10bf >= cs_log10bf_thresh1 & cs2_log10bf >= cs_log10bf_thresh1]

dt2 = dt.sig[!(cs1==1 & cs2==1) & cs1_log10bf >= cs_log10bf_thresh2 & cs2_log10bf >= cs_log10bf_thresh2]


dt.qc = rbind(dt1, dt2)

setcolorder(dt.qc, c("dataset1", "dataset2"))

fwrite(dt.qc, file="colocQC.tsv.gz", sep="\t", na="NA", quote=FALSE)

