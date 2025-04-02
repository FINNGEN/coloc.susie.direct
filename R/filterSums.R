#!/usr/bin/env Rscript

#######################################################
# filter a coloc dataset into colocalizations that pass the filtering.
# Author: Arto Lehisto <arto.lehisto@helsinki.fi>
#######################################################

require(data.table)
require(stringi)

args = commandArgs(TRUE)

fileList = args[1]
h4Thresh = as.numeric(args[2])
cs_log10bf_thresh = as.numeric(args[3])
probmass_threshold = as.numeric(args[4])
out_name = args[5]

files.val = readLines(fileList)

dts = list()
idx = 1
n = length(files.val)
nTotal = 0
for(file1 in files.val){
    message(idx, "/", n, ": ", file1)
    dt = fread(file1)
    nTotal = nTotal + nrow(dt)
    message(" Current: ", nrow(dt), " rows, total: ", nTotal)

    dt.val = dt[PP.H4.abf >= h4Thresh & probmass_1 > probmass_threshold & probmass_2 > probmass_threshold]

    dts[[idx]] = dt.val
    idx = idx + 1
}

dt.sig = rbindlist(dts)
rm(dts)

dt.qc = dt.sig[cs1_log10bf >= cs_log10bf_thresh & cs2_log10bf >= cs_log10bf_thresh]

fwrite(dt.qc, file=out_name, sep="\t", na="NA", quote=FALSE)