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

    call mergeColoc{input: colocs=coloc.res, hits=coloc.hits, credsets=coloc.credset_variants, colocInfo=info, docker=docker,
        h4pp_thresh=h4pp_thresh,cs_log10bf_thresh=cs_log10bf_thresh,probmass_threshold=probmass_threshold}

    call mergeH4Tables{
        input: colocInfo=mergeColoc.coloc_out,h4_variant_tables=coloc.h4_variants,docker=docker
    }

    output{
        File unfiltered_sum = mergeColoc.unfiltered
        File sum = mergeColoc.coloc_out
        File hit = mergeColoc.hit
        File h4_variant = mergeH4Tables.h4_variant
        File credset = mergeColoc.credset
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


task mergeH4Tables{
    input{
        File colocInfo
        Array[String] h4_variant_tables
        String docker
    }
    String out_stub=basename(colocInfo,".pairs.tar.gz")
    command <<<
        set -e
        #set gcloud auth 
        export GCS_AUTH_TOKEN="$(gcloud auth print-access-token)"
        filter_h4.py ~{colocInfo} ~{write_lines(h4_variant_tables)} "~{out_stub}" "~{out_stub}.h4.variants.tsv.gz"
    >>>

    runtime{
        cpu: 1
        memory: "4 GB"
        docker: "~{docker}"
        noAddress: true
        zones: "europe-west1-b"
        disks: "local-disk 100 HDD"
        preemptible: 0
    }

    output{

        File h4_variant = "~{out_stub}.h4.variants.tsv.gz"
    }
}

task mergeColoc{
    input{
        Array[String] colocs
        Array[String] hits
        Array[String] credsets
        String colocInfo
        String docker
        Float h4pp_thresh
        Float cs_log10bf_thresh
        Float probmass_threshold
    }
    String out_stub=basename(colocInfo,".pairs.tar.gz")
    command <<<
        
        echo "~{sep='\n' colocs}" > sum.txt
        echo "~{sep='\n' hits}" > hits.txt
        echo "~{sep='\n' credsets}" > credsets.txt

        mkdir sums h4_variants hits credsets
        
        cat sum.txt | gcloud storage cp -I sums/
        find sums/ -name "*.sum.tsv" > sum_list
        filterSums.R sum_list ~{h4pp_thresh} ~{cs_log10bf_thresh} ~{probmass_threshold} "~{out_stub}.sum.tsv.gz"
        awk 'FNR>1 || NR==1' sums/*.tsv | gzip > ~{out_stub}.sum.unfiltered.tsv.gz
        rm sums/*.tsv

        cat hits.txt | gcloud storage cp -I hits/
        awk 'FNR>1 || NR==1' hits/*.tsv | gzip > ~{out_stub}.hits.tsv.gz
        rm hits/*.tsv

        cat credsets.txt | gcloud storage cp -I credsets/
        find credsets/ -name "*.credsets.tsv" > cs_list
        mergeVariants.py "~{out_stub}.sum.tsv.gz" cs_list "~{out_stub}" "temp_cs.gz" 
        cat <(zcat "temp_cs.gz"|head -n1) <(zcat "temp_cs.gz"|tail -n+2|sort -T ./|uniq)|gzip > temp_cs_2.gz
        mv temp_cs_2.gz "~{out_stub}.credset.tsv.gz"
    >>>

    runtime{
        cpu: 1
        memory: "4 GB"
        docker: "~{docker}"
        noAddress: true
        zones: "europe-west1-b"
        disks: "local-disk 500 HDD"
        preemptible: 0
    }

    output{
        File coloc_out = out_stub+".sum.tsv.gz"
        File hit = out_stub + ".hits.tsv.gz"
        File credset = out_stub+".credset.tsv.gz"
        File unfiltered = out_stub+".sum.unfiltered.tsv.gz"
    }
}
