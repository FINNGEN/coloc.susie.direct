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

processInfo <- function(info_row, side){
    source_val = info_row$source
    tissue_val = info_row$tissue
    quant_val  = info_row$quant

    message("Processing coloc", side, ", ", source_val)

    cols = names(info_row)
    prefix = NA_character_
    if("URL" %in% cols){
        prefix = sub("/+$", "", info_row$URL)
        region_list = paste0(prefix, "/Coloc.regions.tsv")
        mapping = paste0(prefix, "/Coloc.map.txt")
    } else if("regions_file" %in% cols && "mapping_file" %in% cols){
        region_list = info_row$regions_file
        mapping = info_row$mapping_file
        prefix = sub("/+$", "", dirname(region_list))
    } else {
        stop("colocInfo must have either 'URL' or 'regions_file'+'mapping_file' columns")
    }

    name = paste0(source_val, "--", info_row$type, "--", tissue_val, "--", quant_val)

    downFile(region_list, "regions.tsv")
    downFile(mapping, paste0("map", side, ".txt"))

    dt.region = fread("regions.tsv", head=TRUE)
    if(sum(colnames(dt.region) == c("URL", "trait", "region")) != 3){
        stop("The region defination file is invalid, header is not consistent: ", region_list)
    }

    if(!is.na(prefix)){
        dt.region[!grepl("gs://", URL), URL:=paste0(prefix, "/", URL)]
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
    dt.region[, dataset:=source_val]
    dt.region[, tissue:=tissue_val]
    dt.region[, quant:=quant_val]
    if(side != 1){
        sel_col = c("start", "end", "dataset", "tissue", "quant")
        setnames(dt.region, sel_col, paste0(sel_col, side))
    }

    dt.region[, pos:=NULL]

    ret = list()
    ret[["region"]] = dt.region
    ret[["name"]] = name
    return(ret)
}

validateColocInfo <- function(dt, fname){
    required = c("source","type","tissue","quant")
    missing = setdiff(required, colnames(dt))
    if(length(missing) > 0)
        stop("Missing required columns in ", fname, ": ", paste(missing, collapse=", "))
    has_url = "URL" %in% colnames(dt)
    has_files = all(c("regions_file","mapping_file") %in% colnames(dt))
    if(!has_url && !has_files)
        stop("colocInfo file ", fname, " must have column 'URL' or both 'regions_file' and 'mapping_file'")
}

info1_dt = fread(info1_file, header=TRUE)
info2_dt = fread(info2_file, header=TRUE)
validateColocInfo(info1_dt, info1_file)
validateColocInfo(info2_dt, info2_file)

for(i in seq_len(nrow(info1_dt))){
    infos1 = processInfo(info1_dt[i], 1)
    dt1 = unique(infos1[["region"]])
    for(j in seq_len(nrow(info2_dt))){

        infos2 = processInfo(info2_dt[j], 2)
        dt2 = unique(infos2[["region"]])

        message(nrow(dt1), " regions in coloc1")
        message(nrow(dt2), " regions in coloc2")

        out = "pairs.tsv"
        cat(c(paste(info1_dt[i], collapse="\t"), paste(info2_dt[j], collapse="\t")), file="coloc.info", sep="\n")
        tar_name = paste0(infos1[["name"]], "-----", infos2[["name"]], ".pairs.tar.gz")
        n_name = paste0(infos1[["name"]], "-----", infos2[["name"]], ".N.count")

        dt3 = dt1[dt2, .(URL, trait, region, dataset, tissue, quant, i.URL, i.trait, i.region, i.dataset2, i.tissue2, i.quant2), on=.(CHR, start <= end2, end >= start2), nomatch=0]

        setnames(dt3, c("URL", "i.URL"), c("URL1", "URL2"))
        setnames(dt3, c("trait", "i.trait"), c("trait1", "trait2"))
        setnames(dt3, c("region", "i.region"), c("region1", "region2"))
        setnames(dt3, c("dataset", "i.dataset2"), c("dataset1", "dataset2"))
        setnames(dt3, c("tissue", "i.tissue2"), c("tissue1", "tissue2"))
        setnames(dt3, c("quant", "i.quant2"), c("quant1", "quant2"))

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
