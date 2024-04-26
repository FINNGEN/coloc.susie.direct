version 1.0

workflow ColocSusieDirect{
    input{
        File finemapList1
        String mapping1

        File finemapList2
        String mapping2

        Int nPerBatch = 200
        String outPrefix
    }

    Array[String] finemap1 = read_lines(finemapList1)
    Int n1 = length(finemap1)
    Int block1 = ceil(1.0 * n1 / nPerBatch)
    #call debug{input: n=n1, blocks=block1}

    Array[String] finemap2 = read_lines(finemapList2)
    Int n2 = length(finemap2)
    Int block2 = ceil(1.0 * n2 / nPerBatch)

    scatter(blk in range(block1)){
        call munge as munge1 {input: finemapList=finemapList1, mapping=mapping1, nPerBatch=nPerBatch, block=blk}
    }
    call mergeMunge as merge1 {input: munged=munge1.region, out=outPrefix + ".info1.tsv"}

    scatter(blk in range(block2)){
        call munge as munge2 {input: finemapList=finemapList2, mapping=mapping2, nPerBatch=nPerBatch, block=blk}
    }
    call mergeMunge as merge2 {input: munged=munge2.region, out=outPrefix + ".info2.tsv"}

    call colocPair{input: info1=merge1.info, info2=merge2.info, out=outPrefix + ".pairs.tsv"}

    Int n3 = colocPair.N
    Int block3 = ceil(1.0 * n3 / nPerBatch)
    scatter(blk in range(block3)){
       call coloc{input: colocList=colocPair.list, map1=mapping1, map2=mapping2, nPerBatch=nPerBatch, block=blk}
    }

    call mergeColoc{input: colocs=coloc.res, hits=coloc.hits, out=outPrefix}

    output{
        File info1 = merge1.info
        File info2 = merge2.info
        File pair = colocPair.list
        File colocTotal = mergeColoc.coloc
        File colocHits = mergeColoc.hit
    }
    meta{
        authors: ["Zhili"]
        version: "0.0.6"
    }
}


task debug {
    input{
        Int n
        Int blocks
    }

    command <<<
        echo ~{n}
        echo ~{blocks}
    >>>

    runtime{
        cpu: 2
        memory: "2 GB"
        docker: "europe-docker.pkg.dev/finngen-refinery-dev/eu.gcr.io/coloc.susie.direct:0.0.1"
        zones: "europe-west1-b"
        disks: "local-disk 10 HDD"
    }
}

task munge {
    input{
        File finemapList
        File mapping
        Int nPerBatch
        Int block
    }

    command <<<
        indexMap.R ~{finemapList} ~{mapping} ~{nPerBatch} ~{block} 
    >>>

    runtime{
        cpu: 2
        memory: "4 GB"
        docker: "europe-docker.pkg.dev/finngen-refinery-dev/eu.gcr.io/coloc.susie.direct:0.0.1"
        zones: "europe-west1-b"
        disks: "local-disk 10 HDD"
    }

    output{
        File region = "region" + block + ".txt"
    }
}

task mergeMunge{
    input{
        Array[File] munged
        String out
    }

    command <<<
        awk 'FNR>1 || NR==1' ~{sep=' ' munged} > ~{out}
    >>>

    runtime{
        cpu: 1
        memory: "2 GB"
        docker: "europe-docker.pkg.dev/finngen-refinery-dev/eu.gcr.io/coloc.susie.direct:0.0.1"
        zones: "europe-west1-b"
        disks: "local-disk 10 HDD"
    }

    output{
        File info = out
    }
}


task colocPair{
    input{
        File info1
        File info2
        String out
    }

    command <<<
        colocPair.R ~{info1} ~{info2} ~{out}
        wc -l ~{out} | awk '{print $1}' > ~{out}.count
    >>>

    runtime{
        cpu: 2
        memory: "4 GB"
        docker: "europe-docker.pkg.dev/finngen-refinery-dev/eu.gcr.io/coloc.susie.direct:0.0.4"
        zones: "europe-west1-b"
        disks: "local-disk 10 HDD"
    }

    output{
        File list = out
        Int N = read_int(out + ".count")
    }
}

task coloc{
    input{
        File colocList
        File map1
        File map2
        Int nPerBatch
        Int block
    }

    command <<<
        coloc.R ~{colocList} ~{map1} ~{map2} ~{nPerBatch} ~{block}
    >>>

    runtime{
        cpu: 2
        memory: "4 GB"
        docker: "europe-docker.pkg.dev/finngen-refinery-dev/eu.gcr.io/coloc.susie.direct:0.0.6"
        zones: "europe-west1-b"
        disks: "local-disk 20 HDD"
    }

    output{
        File res = "region" + block + ".sum.tsv"
        File hits = "region" + block + ".hits.tsv"
    }
}


task mergeColoc{
    input{
        Array[File] colocs
        Array[File] hits
        String out
    }

    command <<<
        awk 'FNR>1 || NR==1' ~{sep=' ' colocs} > ~{out}.sum.tsv
        awk 'FNR>1 || NR==1' ~{sep=' ' hits} > ~{out}.hits.tsv
    >>>

    runtime{
        cpu: 1
        memory: "2 GB"
        docker: "europe-docker.pkg.dev/finngen-refinery-dev/eu.gcr.io/coloc.susie.direct:0.0.4"
        zones: "europe-west1-b"
        disks: "local-disk 10 HDD"
    }

    output{
        File coloc = out + ".sum.tsv"
        File hit = out + ".hits.tsv"
    }
}
