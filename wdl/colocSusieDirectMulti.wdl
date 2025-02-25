version 1.0

import "colocSusiePair.wdl" as coloc_sub

workflow ColocSusieDirectMulti{
    input{
        File colocInfo1
        File colocInfo2

        Int nColocPerBatch = 1000
        Boolean excludeSameNameTrait = true
        Float h4pp_thresh = 0.5
        Float cs_log10bf_thresh = 0.9
        Float probmass_threshold = 0.9
        String docker = "eu.gcr.io/finngen-sandbox-v3-containers/coloc.susie.direct:0.1.7"
    }

    Array[String] coloc1 = read_lines(colocInfo1)
    Array[String] coloc2 = read_lines(colocInfo2)

    Array[Pair[String, String]] allPair = cross(coloc1, coloc2)

    scatter(pair1 in allPair){
        call generatePair{
            input: coloc1=pair1.left, coloc2=pair1.right, excludeSameNameTrait=excludeSameNameTrait, docker=docker
        }
        if(generatePair.N > 0){
            call coloc_sub.ColocPair as colocPair {
                input: info=generatePair.pairs, N=generatePair.N, nColocPerBatch=nColocPerBatch, docker=docker, h4pp_thresh=h4pp_thresh,cs_log10bf_thresh=cs_log10bf_thresh,probmass_threshold=probmass_threshold
            }
        }
    }
     
    Array[File] allColoc = select_all(colocPair.sum)
    Array[File] allH4Table = select_all(colocPair.h4_variant)
    Array[File] allCredset = select_all(colocPair.credset)
    call mergeAllPair{
        input: colocs=allColoc, h4pp_thresh=h4pp_thresh, cs_log10bf_thresh=cs_log10bf_thresh, docker=docker,probmass_threshold=probmass_threshold
    }

    call mergeVariants{
        input: h4_files = allH4Table, credsets=allCredset,docker=docker
    }

    output{
        Array[File] pairs = generatePair.pairs
        Array[Int] N = generatePair.N
        Array[File] filtered_coloc = allColoc
        Array[File] hit = select_all(colocPair.hit)
        Array[File] unfiltered_coloc = select_all(colocPair.unfiltered_sum)
        File colocCredsets = mergeVariants.colocCredsets
        File colocH4 = mergeVariants.colocH4Tables
        File colocQC = mergeAllPair.colocQC
    } 
}

task generatePair{
    input{
        String coloc1
        String coloc2
        Boolean excludeSameNameTrait
        String docker
    }

    command <<<
        colocPairStr.R "~{coloc1}" "~{coloc2}" ~{excludeSameNameTrait}
    >>>

    runtime{
        cpu: 2
        memory: "4 GB"
        docker: "~{docker}"
        zones: "europe-west1-b"
        disks: "local-disk 10 HDD"
    }

    output{
        File pairs = select_first(glob("*.pairs.tar.gz"))
        Int N = read_int("N.count")
    }
}

task mergeVariants{
    input{
        Array[File] h4_files
        Array[File] credsets
        String docker
    }

    command <<<
        set -e
        # sort and unique the credsets
        cat <(<cat ~{credsets[0]}) <(cat ~{write_lines(credsets)}|xargs -I bash -c "zcat % |tail -n+2" |sort -T ./|uniq)|gzip > coloc.credsets.tsv.gz
        # merge h4 tables
        cat <(<cat ~{h4_files[0]}) <(cat ~{write_lines(h4_files)}|xargs -I bash -c "zcat % |tail -n+2" )|gzip > coloc.H4_tables.tsv.gz
    >>>

    output{
        File colocCredsets = "coloc.credsets.tsv.gz"
        File colocH4Tables = "coloc.H4_tables.tsv.gz"
    }

    runtime{
        cpu: 2
        memory: "4 GB"
        docker: "~{docker}"
        zones: "europe-west1-b europe-west1-c europe-west1-d"
        preemptible: 0
        disks: "local-disk 1000 HDD"
    }

}

task mergeAllPair{
    input{
        Array[File] colocs
        Float h4pp_thresh
        Float cs_log10bf_thresh
        Float probmass_threshold
        String docker
    }

    command <<<
        echo "~{sep='\n' colocs}" > list.txt
        mergeAllPair.R list.txt ~{h4pp_thresh} ~{cs_log10bf_thresh}  ~{probmass_threshold}
    >>>

    runtime{
        cpu: 2
        memory: "4 GB"
        docker: "~{docker}"
        zones: "europe-west1-b"
        preemptible: 0
        disks: "local-disk 100 HDD"
    }
    
    output{
        File colocQC = "colocQC.tsv.gz"
    }
}
