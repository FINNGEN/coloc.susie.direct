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
info1_file = args[1]
# region information for finemap set 2
info2_file = args[2]
# exclude same name trait
bExclude = as.logical(args[3])

downFile <- function(URL, filename, numTries=5){
    command = paste0("gsutil cat ", URL, " > ", filename)
    curTry = 0
    while(system(command) != 0){
        message("Retring...")
        if(curTry >= numTries){
            stop("cannot download ", URL, " to ", filename)
        }
        Sys.sleep(10 * curTry)
        curTry = curTry + 1
    }
}

processInfo <- function(infostr, side){
    message("Processing coloc", side, ", ", infostr)
    infos = strsplit(infostr, "[ \t]+")[[1]]
    infos_val = infos[infos != ""]

    n_info = length(infos_val)
    if(n_info < 3){
        stop("the information is invald: ", infostr)
    }

    name = paste0(infos_val[1], "--", infos_val[2])

    region_list = ""
    mapping = ""
    isPrefix = FALSE
    if(n_info == 3){
        region_list = paste0(infos_val[3], "/Coloc.regions.tsv")
        mapping = paste0(infos_val[3], "/Coloc.map.txt")
        isPrefix = TRUE
    }else if(n_info == 4){
        region_list = infos_val[3]
        mapping = infos_val[4]
    }else{
        stop("the information is invalid: ", infostr)
    }

    system2("gsutil", c("cat", region_list), stdout = "regions.tsv")
    downFile(region_list, "regions.tsv")
    downFile(mapping, paste0("map", side, ".txt"))

    dt.region = fread("regions.tsv", head=TRUE)
    if(sum(colnames(dt.region) == c("URL", "trait", "region")) != 3){
        stop("The region defination file is invalid, header is not consistent: ", region_list)
    }

    if(isPrefix){
        dt.region[!grepl("gs://", URL), URL:=paste0(infos_val[3], "/", URL)]
    }

    if(nrow(dt.region[!grepl("gs://", URL)]) != 0){
        stop("The URL in the region defination file is invalid")
    }
    dt.region[, CHR:=stri_split_fixed(region, ":", simplify=TRUE)[, 1]]
    dt.region[, pos:=stri_split_fixed(region, ":", simplify=TRUE)[, 2]]
    dt.region[, start:=as.numeric(stri_match(pos, regex="^(-?\\d+)-")[, 2])]
    dt.region[, end:=as.numeric(stri_match(pos, regex="-(\\d+)$")[, 2])]
    if(nrow(dt.region[start > end | end < 0]) != 0){
        stop("Some start position in region are larger than end position, or the end position is smaller than 0")
    }
    if(nrow(dt.region[is.na(start) | is.na(end) | is.infinite(start) | is.infinite(end)]) != 0){
        stop("Some invalid start and end position")
    }

    dt.region[!grepl("chr", CHR), CHR:=paste0("chr", CHR)]
    dt.region[, CHR:=gsub("chr23", "chrX", CHR)]
    dt.region[, dataset:=infos_val[1]]
    if(side != 1){
        sel_col = c("start", "end", "dataset")
        setnames(dt.region, sel_col, paste0(sel_col, side))
    }

    dt.region[, pos:=NULL]

    ret = list()
    ret[["region"]] = dt.region
    ret[["name"]] = name
    return(ret)
}

lines_1 = readLines(info1_file)
lines_2 = readLines(info2_file)
for(info1 in lines_1){
    infos1 = processInfo(info1, 1)
    dt1 = unique(infos1[["region"]])
    for(info2 in lines_2){

        infos2 = processInfo(info2, 2)
        dt2 = unique(infos2[["region"]])

        message(nrow(dt1), " regions in coloc1")
        message(nrow(dt2), " regions in coloc2")

        out = "pairs.tsv"
        cat(c(info1, info2), file="coloc.info", sep="\n")
        tar_name = paste0(infos1[["name"]], "-----", infos2[["name"]], ".pairs.tar.gz")
        n_name = paste0(infos1[["name"]], "-----", infos2[["name"]], ".N.count")

        dt3 = dt1[dt2, .(URL, trait, region, dataset, i.URL, i.trait, i.region, i.dataset2), on=.(CHR, start <= end2, end >= start2), nomatch=0]


        setnames(dt3, c("URL", "i.URL"), c("URL1", "URL2"))
        setnames(dt3, c("trait", "i.trait"), c("trait1", "trait2"))
        setnames(dt3, c("region", "i.region"), c("region1", "region2"))
        setnames(dt3, c("dataset", "i.dataset2"), c("dataset1", "dataset2"))

        dt3.ord = dt3[order(dataset1, trait1, region1, dataset2, trait2, region2)]
        message(nrow(dt3.ord), " total pairs have overlapped region.")
        if(bExclude){
            dt3.ord = dt3.ord[dataset1 != dataset2 | trait1 != trait2]
            message(nrow(dt3.ord), " pairs after removing traits with the same name in the same dataset")
        }
        fwrite(dt3.ord, file=out, sep="\t", col.names=F, na=NA)

        cat(nrow(dt3.ord), file=n_name, sep="\n")
        #tar and gzip options to remove all non-reproducible values
        system(paste0("tar --format=gnu  --sort=name --numeric-owner --owner=0 --group=0 --mode='go-rwx,u-rw' --mtime='1970-01-01' --no-recursion --null -cf -  pairs.tsv map1.txt map2.txt coloc.info|gzip --no-name --best > ",tar_name))
    }
}

message("Done")
