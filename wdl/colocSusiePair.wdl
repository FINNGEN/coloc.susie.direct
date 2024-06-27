version 1.0

workflow ColocPair{
    input{
        File info
        Int N
        Int nColocPerBatch
        String docker
    }

    Int block = ceil(1.0 * N / nColocPerBatch)
    scatter(blk in range(block)){
       call coloc{input: colocInfo=info, nPerBatch=nColocPerBatch, block=blk, docker=docker}
    }

    call mergeColoc{input: colocs=coloc.res, hits=coloc.hits, colocInfo=info, docker=docker}

    output{
        File coloc = mergeColoc.coloc
        File hit = mergeColoc.hit
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
        tar xvf ~{colocInfo}
        coloc.R pairs.tsv map1.txt map2.txt ~{nPerBatch} ~{block}
    >>>

    runtime{
        cpu: 2
        memory: "4 GB"
        docker: "~{docker}"
        zones: "europe-west1-b"
        disks: "local-disk 50 HDD"
        noAddress: true
    }

    output{
        File res = "region" + block + ".sum.tsv"
        File hits = "region" + block + ".hits.tsv"
    }
}


task mergeColoc{
    input{
        Array[String] colocs
        Array[String] hits
        String colocInfo
        String docker
    }

    command <<<
        filename=$(basename "~{colocInfo}")
        out="$(echo $filename | sed 's/.pairs.tar.gz//')"
        echo "~{sep='\n' colocs}" > sum.txt
        echo "~{sep='\n' hits}" > hits.txt

        cat sum.txt | gcloud storage cp -I .
        awk 'FNR>1 || NR==1' *.tsv | gzip > ${out}.sum.tsv.gz
        rm *.tsv

        cat hits.txt | gcloud storage cp -I .
        awk 'FNR>1 || NR==1' *.tsv | gzip > ${out}.hits.tsv.gz
    >>>

    runtime{
        cpu: 1
        memory: "2 GB"
        docker: "~{docker}"
        noAddress: true
        zones: "europe-west1-b"
        disks: "local-disk 50 HDD"
        preemptible: 0
    }

    output{
        File coloc = select_first(glob("*.sum.tsv.gz"))
        File hit = select_first(glob("*.hits.tsv.gz"))
    }
}
