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
cs_log10bf_thresh = as.numeric(args[3])
probmass_threshold = as.numeric(args[4])


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
    name1 = basename(file1)

    dt[, colocRes:=name1]
    # dataset1 and dataset2 columns should already exist in the file from coloc.R
    if(!"dataset1" %in% colnames(dt)){
        stop("dataset1 column not found in file: ", file1)
    }
    if(!"dataset2" %in% colnames(dt)){
        stop("dataset2 column not found in file: ", file1)
    }
    nTotal = nTotal + nrow(dt)
    message(" Current: ", nrow(dt), " rows, total: ", nTotal)

    dt.val = dt[PP.H4.abf >= h4Thresh & probmass_1 > probmass_threshold & probmass_2 > probmass_threshold]

    dts[[idx]] = dt.val
}

dt.sig = rbindlist(dts)
rm(dts)


dt.qc = dt.sig[cs1_log10bf >= cs_log10bf_thresh & cs2_log10bf >= cs_log10bf_thresh]

setcolorder(dt.qc, c("dataset1", "dataset2"))

fwrite(dt.qc, file="colocQC.tsv.gz", sep="\t", na="NA", quote=FALSE)

