#!/usr/bin/env Rscript

#######################################################
# Check the finemapping files, validate mapping columns and generate the regions
# This function depends on the google cloud tools, to save the running resource
# Author: Zhili<zhilizheng@outlook.com>
#######################################################


require(data.table)
args =  commandArgs(TRUE)

URLs = args[1]
map1 = args[2]
nPerBlock = as.numeric(args[3])
block = as.numeric(args[4])

message("List: ", URLs)
message("Map: ", map1)

message("N process in current block: ", nPerBlock)
message("Current block: ", block)

urls = readLines(URLs)
nURL = length(urls)
message(nURL, " in list")

start = block * nPerBlock + 1
end = (block + 1) * nPerBlock

if(end > nURL){
    end = nURL
}

if(start > nURL){
    stop("Too large block ", block)
}

output = paste0("region", block, ".txt")
message("Processing from ", start, " to ", end)

urlsBlock = urls[start:end]

dtm = fread(map1, head=F)
setnames(dtm, c("key", "ori"))

nDone = 0
nBlock = length(urlsBlock)
dts_list = list()
for(URL in urlsBlock){
    message(nDone,"/", nBlock,  " current URL: ", URL)
    dt.map = copy(dtm)
    dt2 = fread(cmd=paste0("gsutil cat ", URL, " | zcat | head -n 5"))
    cols2 = colnames(dt2)
    dt.map[key=="lbf_variable_prefix", ori:=paste0(ori, 1)]

    idx = match(dt.map$ori, cols2)

    dt.map[, idx:=idx]
    dt.map1.invalid = dt.map[is.na(idx)]

    if(nrow(dt.map1.invalid) != 0){
        stop("Check the heads in ", URL, " with the mapping ", map1, " failed")
    }

    col_region = dt.map[key=="region"]$idx[1]
    col_trait = dt.map[key=="trait"]$idx[1]

    dt.region = fread(cmd=paste0("gsutil cat ", URL, " | zcat | ", "awk 'FNR>1 && !a[$", col_trait, ",$", col_region, "]++{print $",col_trait, ",$", col_region, "}'"), head=F)
    setnames(dt.region, c("trait", "region"))

    # has duplicated trait
    if(length(unique(dt.region$trait)) != 1){
        stop("Has duplicted traits in the ", URL)
    }

    dt.ret = cbind(data.table(URL=URL), dt.region)

    nDone = nDone + 1
    dts_list[[nDone]] = dt.ret
}

dts = rbindlist(dts_list)
fwrite(dts, file=output, sep="\t")

message("Done")

