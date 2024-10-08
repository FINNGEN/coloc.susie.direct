#!/usr/bin/env Rscript

#######################################################
# coloc in a block
# This function depends on the google cloud tools, to save the running resource
# Author: Zhili<zhilizheng@outlook.com>
#######################################################

require(data.table)
require(coloc)

args = commandArgs(TRUE)
# coloc list file
colocList=args[1]
# mapping 
map1=args[2]
# mapping
map2=args[3]
# how many lines to run
nPerBlock=as.numeric(args[4])
# current running block
block=as.numeric(args[5])

dt.list = fread(colocList, head=F)
dt.map1 = fread(map1, head=F)
dt.map2 = fread(map2, head=F)

setnames(dt.list, c("URL1", "trait1", "region1", "URL2", "trait2", "region2"))

nList = nrow(dt.list)
message(nList, " in list")
message("N process in current block: ", nPerBlock)
message("Current block: ", block)

start = block * nPerBlock + 1
end = (block + 1) * nPerBlock

if(end > nList){
    end = nList
}

if(start > end){
    stop("Too large block, ", block, ", the start and end is not right")
}

output = paste0("region", block, ".sum.tsv")
output.hits = paste0("region", block, ".hits.tsv")
message("Processing from ", start, " to ", end)

dt.list = dt.list[start:end]
nList = nrow(dt.list)

dt.list[, out1:=paste0(trait1, "---", gsub(":", ".", region1), ".txt")]
dt.list[, out2:=paste0(trait2, "---", gsub(":", ".", region2), ".txt")]

##################
# extract geno
#################

grabRegion <- function(url, region, headout, out, maxRetry=5){
    command = paste0("cp ", headout, " ", out, " && tabix ", url, " ", region, " >> ", out, " && gzip ", out)
    tryTimes = 0
    while(system(command) != 0){
        tryTimes = tryTimes + 1
        message("Retrying ", tryTimes)
        token = system("gcloud auth print-access-token", intern = TRUE)
        Sys.setenv(GCS_OAUTH_TOKEN=token)
        if(tryTimes > maxRetry){
            stop("Error: can't processing file ", url, " ", region)
        }
    }
}

# trait1
message("Extracting finemaping results...")
message("Set 1")
#token = system("gcloud auth print-access-token", intern = TRUE)
dt3.cur1 = dt.list[!duplicated(out1)]
n = nrow(dt3.cur1)

url = dt3.cur1$URL1[1]
headout = paste0("header1")

system(paste0("gsutil cat ", url, " | zcat | head -n 1 > ", headout))

for(idx in 1:n){
    message(idx, ", ", n)
    dt.t1 = dt3.cur1[idx]
    url = dt.t1$URL1
    out = dt.t1$out1
    region = dt.t1$region1
    if(!file.exists(paste0(out, ".gz"))){
        grabRegion(url, region, headout, out)
    }
}

####################
# trait2
message("Set 2")
dt3.cur2 = dt.list[!duplicated(out2)]
n = nrow(dt3.cur2)
url = dt3.cur2$URL2[1]
headout = paste0("header2")

system(paste0("gsutil cat ", url, " | zcat | head -n 1 > ", headout))

for(idx in 1:n){
    message(idx, ", ", n)
    dt.t1 = dt3.cur2[idx]
    url = dt.t1$URL2
    out = dt.t1$out2
    region = dt.t1$region2
    if(!file.exists(paste0(out, ".gz"))){
        grabRegion(url, region, headout, out)
    }
}

##########################
# coloc
message("Coloc...")
dts = list()
dts.hits = list()
nProcess = 0
lbf1 = dt.map1[V1=="lbf_variable_prefix"]$V2
lbf2 = dt.map2[V1=="lbf_variable_prefix"]$V2
for(f1 in dt3.cur1$out1){
    # prepare the first one
    dt1 = fread(paste0(f1, ".gz"))
    cols1 = colnames(dt1)
    lbf_cols1 = cols1[grepl(lbf1, cols1)]
    lbfn_cols1 = gsub(lbf1, "lbf1_", lbf_cols1)
    setnames(dt1, lbf_cols1, lbfn_cols1)
    dt.map1.use = dt.map1[V1!="lbf_variable_prefix"]
    dt.map1.use[!V1 %in% c("rsid"), V1:=paste0(V1, "1")]
    setnames(dt1, dt.map1.use$V2, dt.map1.use$V1)

    use_cols1 = c(dt.map1.use$V1, lbfn_cols1)
    dt1.use = dt1[, ..use_cols1]

    dt.list.cur = dt.list[out1==f1]

    for(f2 in dt.list.cur$out2){
        nProcess = nProcess + 1
        message(nProcess, "/", nList)
        
        dt2 = fread(paste0(f2, ".gz"))
        cols2 = colnames(dt2)
        lbf_cols2 = cols2[grepl(lbf2, cols2)]
        lbfn_cols2 = gsub(lbf2, "lbf2_", lbf_cols2)
        setnames(dt2, lbf_cols2, lbfn_cols2)

        dt.map2.use = dt.map2[V1!="lbf_variable_prefix"]
        dt.map2.use[!V1 %in% c("rsid"), V1:=paste0(V1, "2")]
        setnames(dt2, dt.map2.use$V2, dt.map2.use$V1)

        use_cols2 = c(dt.map2.use$V1, lbfn_cols2)
        dt3 = merge(dt1.use, dt2[, ..use_cols2], by=c("rsid"))

        cs1 = unique(dt3$cs1)
        cs2 = unique(dt3$cs2)
        cs1 = cs1[cs1 != -1]
        cs2 = cs2[cs2 != -1]

        dt.sum1 = data.table()
        dt.hit1 = data.table()
        if(length(cs1) == 0 || length(cs2) == 0 ){
            message("No valid cs")
        }else{
            message("Valid cs")
            sel_lbf_cols1 = paste0("lbf1_", cs1)
            sel_lbf_cols2 = paste0("lbf2_", cs2)

            bf1_rec = t(as.matrix(dt3[, ..sel_lbf_cols1]))
            colnames(bf1_rec) <- dt3$rsid

            bf2_rec = t(as.matrix(dt3[, ..sel_lbf_cols2]))
            colnames(bf2_rec) <- dt3[["rsid"]]

            ret1 = coloc.bf_bf(bf1_rec,bf2_rec)

            #saveRDS(ret1, file=paste0(outPrefix, ".rds"))
            #saveRDS(dt3, file=paste0(outPrefix, ".dt.rds"))

            dt.sum = ret1$summary
            if(!is.null(dt.sum)){
                dt3.1 = dt3[!duplicated(cs1)][cs1!= -1, .(cs1, low_purity1)]
                dt3.2 = dt3[!duplicated(cs2)][cs2!= -1, .(cs2, low_purity2)]

                dt.sum[, idx1:=cs1[idx1]]
                dt.sum[, idx2:=cs2[idx2]]

                dt.sum1 = merge(merge(dt.sum, dt3.1, by.x="idx1", by.y="cs1"), dt3.2, by.x="idx2", by.y="cs2")
                dt.sum1$trait1 = dt3[1]$trait1
                dt.sum1$trait2 = dt3[1]$trait2
                dt.sum1$region1 = dt3[1]$region1
                dt.sum1$region2 = dt3[1]$region2
                dt.sum1$nsnps1 = nrow(dt1)
                dt.sum1$nsnps2 = nrow(dt2)
                setnames(dt.sum1, c("idx1", "idx2"), c("cs1", "cs2"))

                hits = unique(c(dt.sum1$hit1, dt.sum1$hit2))
                dt.hit1 = dt3[rsid %in% hits]

                # clpp, clpa, clpm
                dt.sum1[, clpp:=NA_real_]
                dt.sum1[, clpa:=NA_real_]
                dt.sum1[, clpm:=NA_real_]
                dt.sum1[, cs1_size:=NA_real_]
                dt.sum1[, cs2_size:=NA_real_]
                dt.sum1[, cs_overlap:=NA_real_]

                dt3[, pp:=pip1*pip2]
                dt3[, pa:=pmin(pip1, pip2)]

                for(idx in 1:nrow(dt.sum1)){
                    dt.sum1.cur = dt.sum1[idx]
                    idx1 = dt.sum1.cur$cs1
                    idx2 = dt.sum1.cur$cs2
                    dt3.cur = dt3[cs1==idx1 & cs2==idx2]
                    dt.sum1$cs1_size[idx] = nrow(dt1[cs1==idx1])
                    dt.sum1$cs2_size[idx] = nrow(dt2[cs2==idx2])
                    dt.sum1$cs_overlap[idx] = nrow(dt3.cur)
                    if(nrow(dt3.cur) != 0){
                        dt.sum1$clpp[idx] = sum(dt3.cur$pp, na.rm=TRUE)
                        dt.sum1$clpa[idx] = sum(dt3.cur$pa, na.rm=TRUE)
                        dt.sum1$clpm[idx] = max(sum(dt3.cur$pip1, na.rm=TRUE), sum(dt3.cur$pip2, na.rm=TRUE))
                    }
                }
            }
        }
        dts[[nProcess]] = dt.sum1
        dts.hits[[nProcess]] = dt.hit1
    }
}


dt.coloc = rbindlist(dts)
dt.hits = rbindlist(dts.hits, fill=TRUE)
if(nrow(dt.coloc) != 0){
    setcolorder(dt.coloc, c("trait1", "trait2", "region1", "region2", "cs1", "cs2"))
}
fwrite(dt.coloc, file=output, sep="\t", na="NA", quote=F)
fwrite(dt.hits, file=output.hits, sep="\t", na="NA",quote=F)
message("Done")
