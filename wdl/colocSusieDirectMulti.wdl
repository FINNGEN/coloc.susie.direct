version 1.0

import "colocSusiePair.wdl" as coloc_sub

workflow ColocSusieDirectMulti{
    input{
        File colocInfo1
        File colocInfo2

        Int nColocPerBatch = 1000
        Boolean excludeSameNameTrait = true
    }

    Array[String] coloc1 = read_lines(colocInfo1)
    Array[String] coloc2 = read_lines(colocInfo2)

    Array[Pair[String, String]] allPair = cross(coloc1, coloc2)

    scatter(pair1 in allPair){
        call generatePair{
            input: coloc1=pair1.left, coloc2=pair1.right, excludeSameNameTrait=excludeSameNameTrait
        }
    }

    Array[File] processPairs = generatePair.pairs
    Array[Int] Ns = generatePair.N

    Int totalPairs = length(processPairs)

    scatter(idx in range(totalPairs)){
        Int curN = Ns[idx]
        if(curN > 0){
            call coloc_sub.ColocPair as colocPair {
                input: info=pairs[idx], N=curN, nColocPerBatch=nColocPerBatch
            }
        }
    }

    output{
        Array[File] pairs = generatePair.pairs
        Array[Int] N = generatePair.N
        Array[File?] coloc = colocPair.coloc
        Array[File?] hit = colocPair.hit
    } 
}

task generatePair{
    input{
        String coloc1
        String coloc2
        Boolean excludeSameNameTrait
    }

    command <<<
        colocPairStr.R "~{coloc1}" "~{coloc2}" ~{excludeSameNameTrait}
    >>>

    runtime{
        cpu: 2
        memory: "4 GB"
        docker: "europe-docker.pkg.dev/finngen-refinery-dev/eu.gcr.io/coloc.susie.direct:0.1.2"
        zones: "europe-west1-b"
        disks: "local-disk 10 HDD"
    }

    output{
        File pairs = select_first(glob("*.pairs.tar.gz"))
        Int N = read_int("N.count")
    }
}
