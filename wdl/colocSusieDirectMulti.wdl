version 1.0

import "colocSusiePair.wdl" as coloc_sub

workflow colocsusiedirectmulti{
    input{
        File colocInfo1
        File colocInfo2

        Int nColocPerBatch = 1000
        Boolean excludeSameNameTrait = true
        Float h4pp_thresh = 0.5
        Float cs_log10bf_thresh1 = 0.9
        Float cs_log10bf_thresh2 = 1.0
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
                input: info=generatePair.pairs, N=generatePair.N, nColocPerBatch=nColocPerBatch, docker=docker
            }
        }
    }
     
    Array[File] allColoc = select_all(colocPair.coloc)

    call mergeAllPair{
        input: colocs=allColoc, h4pp_thresh=h4pp_thresh, cs_log10bf_thresh1=cs_log10bf_thresh1, cs_log10bf_thresh2=cs_log10bf_thresh2, docker=docker
    }

    output{
        Array[File] pairs = generatePair.pairs
        Array[Int] N = generatePair.N
        Array[File?] coloc = colocPair.coloc
        Array[File?] hit = colocPair.hit
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

task mergeAllPair{
    input{
        Array[File] colocs
        Float h4pp_thresh
        Float cs_log10bf_thresh1 
        Float cs_log10bf_thresh2
        String docker
    }

    command <<<
        echo "~{sep='\n' colocs}" > list.txt
        mergeAllPair.R list.txt ~{h4pp_thresh} ~{cs_log10bf_thresh1} ~{cs_log10bf_thresh2}
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
