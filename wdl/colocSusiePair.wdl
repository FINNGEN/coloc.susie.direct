version 1.0

workflow ColocPair{
    input{
        File info
        Int N
        Int nColocPerBatch
        String docker
        Float h4pp_thresh
        Float cs_log10bf_thresh
        Float probmass_threshold
    }

    Int block = ceil(1.0 * N / nColocPerBatch)
    scatter(blk in range(block)){
       call coloc{input: colocInfo=info, nPerBatch=nColocPerBatch, block=blk, docker=docker}
    }

    call mergeColoc{input: colocs=coloc.res, hits=coloc.hits, variants=coloc.variants, colocInfo=info, docker=docker,
        h4pp_thresh=h4pp_thresh,cs_log10bf_thresh=cs_log10bf_thresh,probmass_threshold=probmass_threshold}

    output{
        File unfiltered_coloc = mergeColoc.unfiltered
        File coloc = mergeColoc.coloc
        File hit = mergeColoc.hit
        File h4_variant = mergeColoc.h4_variant
        File credset = mergeColoc.credset
        #File variant = mergeColoc.variant
    }

    meta{
        authors: ["Zhili"]
        version: "0.1.7"
    }
}


task coloc{
    input{
        File colocInfo
        Int nPerBatch
        Int block
        String docker
    }

    command <<<
        filename=$(basename "~{colocInfo}")
        dataset_id="$(echo $filename | sed 's/.pairs.tar.gz//')"
        #this is needed in the coloc.R command, since it has commands consisting of piped commands, which do not show correct return code without pipefail
        set -o pipefail
        tar xvf ~{colocInfo}
        coloc.R pairs.tsv map1.txt map2.txt ~{nPerBatch} ~{block} "${dataset_id}"
        #save 
        
        #make sure credset vars are unique
        cat <(head -n1 "region~{block}.credsets.tsv") <(tail -n+2 "region~{block}.credsets.tsv"|sort|uniq) > vars2
        mv vars2 "region~{block}.credsets.tsv"
    >>>

    runtime{
        cpu: 2
        memory: "4 GB"
        docker: "~{docker}"
        zones: "europe-west1-b"
        disks: "local-disk 100 HDD"
        noAddress: true
    }

    output{
        File res = "region" + block + ".sum.tsv"
        File hits = "region" + block + ".hits.tsv"
        File credset_variants = "region" + block + ".credsets.tsv"
        File h4_variants = "region" + block + ".h4_variants.tsv"
    }
}


task mergeColoc{
    input{
        Array[String] colocs
        Array[String] hits
        Array[String] h4_variants
        Array[String] credsets
        String colocInfo
        String docker
        Float h4pp_thresh
        Float cs_log10bf_thresh
        Float probmass_threshold
    }

    command <<<
        filename=$(basename "~{colocInfo}")
        out="$(echo $filename | sed 's/.pairs.tar.gz//')"
        echo "~{sep='\n' colocs}" > sum.txt
        echo "~{sep='\n' hits}" > hits.txt
        echo "~{sep='\n' h4_variants}" > h4_variants.txt
        echo "~{sep='\n' credsets}" > credsets.txt

        mkdir sums h4_variants hits credsets
        
        cat sum.txt | gcloud storage cp -I sums/
        find sums/ -name "*.sum.tsv" > sum_list
        filterSums.R sum_list ~{h4pp_thresh} ~{cs_log10bf_thresh} ~{probmass_threshold} "${out}.sum.tsv.gz"
        awk 'FNR>1 || NR==1' sums/*.tsv | gzip > ${out}.sum.unfiltered.tsv.gz
        rm sums/*.tsv

        cat hits.txt | gcloud storage cp -I hits/
        awk 'FNR>1 || NR==1' hits/*.tsv | gzip > ${out}.hits.tsv.gz
        rm hits/*.tsv
        
        cat h4_variants.txt| gcloud storage cp -I h4_variants/
        awk 'FNR>1 || NR==1' h4_variants/*.tsv | gzip > ${out}.h4_variants.tsv.gz
        rm h4_variants/*.tsv

        cat credsets.txt | gcloud storage cp -I credsets/
        find credsets/ -name "*.credsets.tsv" > cs_list
        python3 mergeVariants.py "${out}.sum.tsv.gz" cs_list "${out}" "${out}.credset.tsv.gz" 
        cat <(zcat "${out}.credset.tsv.gz"|head -n1) <(zcat "${out}.credset.tsv.gz"|tail -n+2|sort -T ./|uniq)|gzip > cs_2
        mv cs_2 "${out}.credset.tsv.gz"
    >>>

    runtime{
        cpu: 1
        memory: "4 GB"
        docker: "~{docker}"
        noAddress: true
        zones: "europe-west1-b"
        disks: "local-disk 1000 HDD"
        preemptible: 0
    }

    output{
        File coloc = select_first(glob("*.sum.tsv.gz"))
        File hit = select_first(glob("*.hits.tsv.gz"))
        File h4_variant = select_first(glob("*.h4_variants.tsv.gz"))
        File credset = select_first(glob("*.credset.tsv.gz"))
        File unfiltered = select_first(glob("*.sum.unfiltered.tsv.gz"))
    }
}
