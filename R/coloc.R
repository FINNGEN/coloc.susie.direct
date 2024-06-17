#!/usr/bin/env Rscript

#######################################################
# coloc in a block
# This function depends on the google cloud tools, to save the running resource
# Author: Zhili<zhilizheng@outlook.com>
#######################################################

require(data.table)
require(stringi)
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

debug = FALSE
if(length(args) >= 6){
    debug = as.logical(args[6])
}

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
    message("Fecthing ", region, " from ", url)
    line = grabTabix(url, region, headout, out, maxRetry)
    message("  Extracted ", line -1)
    if(line == 1){
        message("--No variant in the region ", region, " from ", url)
        tries = c(region)
        region1 = region
        if(!grepl("chr", region)){
            region1 = paste0("chr", region)
        }
        if(region1 != region){
            tries = c(tries, region1)
        }
        region2 = gsub("chr23", "chrX", region1)
        if(region2 != region1){
            tries = c(tries, region2)
        }
        region3 = gsub("chrX", "chr23", region1)
        if(region3 != region1){
            tries = c(tries, region3)
        }

        region_nochr = gsub("chr", "", tries)
        region_all = c(region, tries, region_nochr)
        region_all_uni = unique(region_all)
        region_all_uni_ex = region_all_uni[region_all_uni != region]
        message("  Trying ", paste0(region_all_uni_ex, collapse=","))
        for(region1 in region_all_uni_ex){
            message("  ", region1)
            line = grabTabix(url, region1, headout, out, maxRetry)
            if(line > 1){
                message("  Extracted ", line -1, " variant")
                return(1)
            }else{
                message("  found no variant")
            }
        }
        message("  Didn't find any variants in this region")
    }
}

grabTabix <- function(url, region, headout, out, maxRetry=5){
    #command = paste0("cp ", headout, " ", out, " && tabix ", url, " ", region, " >> ", out, " && gzip ", out)
    command = paste0("cp ", headout, " ", out, " && tabix ", url, " ", region, " >> ", out)
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
    return(as.numeric(system(paste0("wc -l ", out, " | awk '{print $1}'"), intern = TRUE)))
}

# trait1
message("\nExtracting finemaping results...")
message("Finemap set 1:")
#token = system("gcloud auth print-access-token", intern = TRUE)
dt3.cur1 = dt.list[!duplicated(out1)]
n = nrow(dt3.cur1)

url = dt3.cur1$URL1[1]
headout = paste0("header1")

system(paste0("gsutil cat ", url, " | zcat | head -n 1 > ", headout))

for(idx in 1:n){
    message(idx, "/", n)
    dt.t1 = dt3.cur1[idx]
    url = dt.t1$URL1
    out = dt.t1$out1
    region = dt.t1$region1
    if(!file.exists(paste0(out))){
        grabRegion(url, region, headout, out)
    }
}

####################
# trait2
message("Finemap set 2:")
dt3.cur2 = dt.list[!duplicated(out2)]
n = nrow(dt3.cur2)
url = dt3.cur2$URL2[1]
headout = paste0("header2")

system(paste0("gsutil cat ", url, " | zcat | head -n 1 > ", headout))

for(idx in 1:n){
    message(idx, "/", n)
    dt.t1 = dt3.cur2[idx]
    url = dt.t1$URL2
    out = dt.t1$out2
    region = dt.t1$region2
    if(!file.exists(paste0(out))){
        grabRegion(url, region, headout, out)
    }
}

##########################
# coloc
# get lbf for cs from lbf_variable
get_cs_lbf = function(dt, cs){
    prior_weights = 1/nrow(dt)
    ext_weight = log(prior_weights + sqrt(.Machine$double.eps))
    lbf_cs = c()
    for(cur_cs in cs){
        cur_lbf_col = paste0("lbf1_", cur_cs)
        if(!cur_lbf_col %in% colnames(dt)){
            cur_lbf_col = paste0("lbf2_", cur_cs)
        }

        lbf_var = dt[[cur_lbf_col]]

        lpo = lbf_var + ext_weight

        maxlpo = max(lpo)

        w_weighted = exp(lpo - maxlpo)
        weighted_sum_w = sum(w_weighted)

        lbf_model = maxlpo + log(weighted_sum_w)

        lbf_cs = c(lbf_cs, log10(exp(lbf_model)))
    }
    ret = list()
    ret[["lbf"]] = data.table(cs=cs, cs_log10bf=lbf_cs)
    ret
}

message("\nColoc...")
dts = list()
dts.hits = list()
nProcess = 0
lbf1 = dt.map1[V1=="lbf_variable_prefix"]$V2
lbf2 = dt.map2[V1=="lbf_variable_prefix"]$V2
for(f1 in dt3.cur1$out1){
    # prepare the first one
    dt.list.cur = dt.list[out1==f1]
    region1.cur = dt.list.cur$region1[1]

    dt1 = fread(paste0(f1))
    cols1 = colnames(dt1)
    lbf_cols1 = cols1[grepl(lbf1, cols1)]
    lbfn_cols1 = gsub(lbf1, "lbf1_", lbf_cols1)
    setnames(dt1, lbf_cols1, lbfn_cols1)
    dt.map1.use = dt.map1[V1!="lbf_variable_prefix"]
    dt.map1.use[!V1 %in% c("rsid"), V1:=paste0(V1, "1")]
    setnames(dt1, dt.map1.use$V2, dt.map1.use$V1)
    dt1[, rsid:=gsub("chr23", "chrX", rsid)]
    dt1 = dt1[region1 == region1.cur]

    use_cols1 = c(dt.map1.use$V1, lbfn_cols1)
    dt1.use = dt1[, ..use_cols1]
    #saveRDS(dt1.use, file=paste0("dt1.rds"))


    for(idx.f2 in 1:nrow(dt.list.cur)){
        f2 = dt.list.cur$out2[idx.f2]
        region2.cur = dt.list.cur$region2[idx.f2]
        nProcess = nProcess + 1
        message("======", nProcess, "/", nList, ": ", f1, ", ", f2)
        
        dt2 = fread(paste0(f2))
        cols2 = colnames(dt2)
        lbf_cols2 = cols2[grepl(lbf2, cols2)]
        lbfn_cols2 = gsub(lbf2, "lbf2_", lbf_cols2)
        setnames(dt2, lbf_cols2, lbfn_cols2)

        dt.map2.use = dt.map2[V1!="lbf_variable_prefix"]
        dt.map2.use[!V1 %in% c("rsid"), V1:=paste0(V1, "2")]
        setnames(dt2, dt.map2.use$V2, dt.map2.use$V1)
        dt2[, rsid:=gsub("chr23", "chrX", rsid)]
        dt2 = dt2[region2==region2.cur]

        use_cols2 = c(dt.map2.use$V1, lbfn_cols2)
        dt2.use = dt2[, ..use_cols2]
        dt3 = merge(dt1.use, dt2.use, by=c("rsid"))
        n_dt3 = nrow(dt3)
        message(n_dt3, " varints in common, ", nrow(dt1), " in dt1, ", nrow(dt2), " in dt2")

        #saveRDS(dt2[, ..use_cols2], file=paste0("dt2.rds"))
        if(debug){
            save(dt1.use, dt2.use, dt3, file=paste0("out.rda"))
        }

        cs1 = unique(dt1.use$cs1)
        cs2 = unique(dt2.use$cs2)
        cs1 = cs1[is.finite(cs1) & cs1 != -1]
        cs2 = cs2[is.finite(cs2) & cs2 != -1]


        dt.sum1 = data.table()
        dt.hit1 = data.table()
        if(nrow(dt3) == 0 || length(cs1) == 0 || length(cs2) == 0 ){
            message("Invalid cs, common SNPs: ", nrow(dt3), ", size cs1: ", length(cs1), ", cs2: ", length(cs2))
        }else{
            message("Valid cs")
            # sort the cs
            cs1 = sort(cs1)
            cs2 = sort(cs2)
            sel_lbf_cols1 = paste0("lbf1_", cs1)
            sel_lbf_cols2 = paste0("lbf2_", cs2)

            ##### for the coloc
            bf1_rec = t(as.matrix(dt3[, ..sel_lbf_cols1]))
            colnames(bf1_rec) <- dt3$rsid

            bf2_rec = t(as.matrix(dt3[, ..sel_lbf_cols2]))
            colnames(bf2_rec) <- dt3[["rsid"]]

            ret1 = coloc.bf_bf(bf1_rec,bf2_rec)
            if(debug){
                saveRDS(ret1, file="ret1.rds")
            }

            dt.sum = ret1$summary
            if(!is.null(dt.sum)){
                message("Valid coloc results")
                dt3.1 = dt1.use[!duplicated(cs1)][cs1!= -1, .(cs1, low_purity1)]
                dt3.2 = dt2.use[!duplicated(cs2)][cs2!= -1, .(cs2, low_purity2)]

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

                # log10lbf for cs
                dt.lbf1 = get_cs_lbf(dt1, cs1)$lbf
                dt.lbf2 = get_cs_lbf(dt2, cs2)$lbf
                setnames(dt.lbf1, gsub("cs", "cs1", colnames(dt.lbf1)))
                setnames(dt.lbf2, gsub("cs", "cs2", colnames(dt.lbf2)))

                dt.sum1 = merge(dt.sum1, dt.lbf1, by="cs1")
                dt.sum1 = merge(dt.sum1, dt.lbf2, by="cs2")


                hits = unique(c(dt.sum1$hit1, dt.sum1$hit2))
                dt.hit1 = dt3[rsid %in% hits]

                # clpp, clpa, clpm
                dt.sum1[, clpp:=NA_real_]
                dt.sum1[, clpa:=NA_real_]
                dt.sum1[, cs1_size:=NA_integer_]
                dt.sum1[, cs2_size:=NA_integer_]
                dt.sum1[, cs_overlap:=NA_integer_]
                dt.sum1[, topInOverlap:=NA_character_]
                dt.sum1[, hit1_info:=NA_character_]
                dt.sum1[, hit2_info:=NA_character_]

                dt3[, pp:=pip1*pip2]
                dt3[, pa:=pmin(pip1, pip2)]
                dt3[, pos:=as.numeric(stri_split_fixed(rsid, "_", simplify=TRUE)[, 2])]
                dt1.use[, pos:=as.numeric(stri_split_fixed(rsid, "_", simplify=TRUE)[, 2])]
                dt2.use[, pos:=as.numeric(stri_split_fixed(rsid, "_", simplify=TRUE)[, 2])]

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
                    }

                    # position check
                    inRegion1 = 0
                    inRegion2 = 0

                    var1 = paste0("lbf1_", idx1)
                    pos1 = dt1.use[get(var1)==max(get(var1))]$pos[1]

                    var2 = paste0("lbf2_", idx2)
                    pos2 = dt2.use[get(var2)==max(get(var2))]$pos[1]

                    pos_min_com = min(dt3$pos)
                    pos_max_com = max(dt3$pos)

                    if(pos1 >= pos_min_com & pos1 <= pos_max_com){
                        inRegion1 = 1
                    }

                    if(pos2 >= pos_min_com & pos2 <= pos_max_com){
                        inRegion2 = 1
                    }

                    dt.sum1$topInOverlap[idx] = paste0(inRegion1, ",", inRegion2)
                    dt.hit1.1 = dt.hit1[rsid == dt.sum1$hit1[idx]]
                    dt.sum1$hit1_info[idx] = paste0(c(dt.hit1.1$beta1, dt.hit1.1$p1), collapse=",")
                    dt.hit1.2 = dt.hit1[rsid == dt.sum1$hit2[idx]]
                    dt.sum1$hit2_info[idx] = paste0(c(dt.hit1.2$beta2, dt.hit1.2$p2), collapse=",")
                }
            }else{
                message(" Invalid coloc results")
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
