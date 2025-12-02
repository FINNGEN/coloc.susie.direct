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
    String zone = "europe-west1-b europe-west1-c europe-west1-d"
    call generatePair{
            input: coloc1=colocInfo1, coloc2=colocInfo2, excludeSameNameTrait=excludeSameNameTrait, docker=docker,zone=zone
    }
    call sort_lists{
        input: cloud_tar_names = generatePair.pairs,cloud_n_names=generatePair.N,tar_suffix=".pairs.tar.gz",n_suffix=".N.count",docker=docker,zone=zone
    }
    scatter(idx in range(length(sort_lists.tar_in_order))){
        Int N_int = read_int(sort_lists.count_in_order[idx])
        if(N_int > 0){
            call coloc_sub.ColocPair as colocPair {
                input: info=sort_lists.tar_in_order[idx], N=N_int, nColocPerBatch=nColocPerBatch, docker=docker, h4pp_thresh=h4pp_thresh,cs_log10bf_thresh=cs_log10bf_thresh,probmass_threshold=probmass_threshold,zone=zone
            }
        }
    }
     
    Array[File] allColoc = select_all(colocPair.sum)
    Array[File] allH4Table = select_all(colocPair.h4_variant)
    Array[File] allCredset = select_all(colocPair.credset)
    call mergeAllPair{
        input: colocs=allColoc, h4pp_thresh=h4pp_thresh, cs_log10bf_thresh=cs_log10bf_thresh, docker=docker,probmass_threshold=probmass_threshold,zone=zone
    }

    call mergeVariants{
        input: h4_files = allH4Table, credsets=allCredset,docker=docker,zone=zone
    }

    output{
        Array[File] pairs = sort_lists.tar_in_order
        Array[Int] N = N_int
        Array[File] filtered_coloc = allColoc
        Array[File] hit = select_all(colocPair.hit)
        Array[File] unfilteredColoc = select_all(colocPair.unfiltered_sum)
        File colocCredsets = mergeVariants.colocCredsets
        File colocH4 = mergeVariants.colocH4Tables
        File colocQC = mergeAllPair.colocQC
        Array[File] unfilteredCredsets = flatten(select_all(colocPair.unfiltered_credsets))
    } 
}

task generatePair{
    input{
        File coloc1
        File coloc2
        Boolean excludeSameNameTrait
        String docker
        String zone
    }

    command <<<
        colocPairStr.R "~{coloc1}" "~{coloc2}" ~{excludeSameNameTrait}
    >>>

    runtime{
        cpu: 2
        memory: "4 GB"
        docker: "~{docker}"
        zones: "~{zone}"
        disks: "local-disk 50 HDD"
        preemptible: 2
    }

    output{
        Array[File] pairs = glob("*.pairs.tar.gz")
        Array[File] N = glob("*.N.count")
    }
}

task sort_lists{
    input{
        Array[String] cloud_tar_names
        Array[String] cloud_n_names
        String tar_suffix
        String n_suffix
        String docker
        String zone
    }

    command <<<
        cat << "__EOF__" > data_in_order.py
        # imports
        import re
        # inputs
        tar_suffix = "~{tar_suffix}"
        n_suffix = "~{n_suffix}"
        tarflist = "~{write_lines(cloud_tar_names)}"
        nflist = "~{write_lines(cloud_n_names)}"
        with open(tarflist,encoding="utf-8") as f: tars=[a.strip() for a in f.readlines()]
        with open(nflist,encoding="utf-8") as f: counts=[a.strip() for a in f.readlines()]
        #make map from basename to path for both of them
        base_ = lambda x:re.sub(".*/","",x)
        
        tar_map = {base_(a).removesuffix(tar_suffix):a for a in tars}
        count_map = {base_(a).removesuffix(n_suffix):a for a in counts}

        assert sorted(tar_map.keys())==sorted(count_map.keys()), "The lists are not same in tars and counts!!! Error"
        # put in same order
        sorted_bases = sorted(tar_map.keys())
        tars_in_order = [tar_map[a] for a in sorted_bases]
        counts_in_order = [count_map[a] for a in sorted_bases]
        # write outputs
        with open("tar_in_order","w",encoding="utf-8") as of:
            for v in tars_in_order:
                of.write(f"{v}\n")
        with open("count_in_order","w",encoding="utf-8") as of:
            for v in counts_in_order:
                of.write(f"{v}\n")
        __EOF__
        python3 data_in_order.py
    >>>

    runtime{
        cpu: 1
        memory: "2 GB"
        docker: "~{docker}"
        zones: "~{zone}"
        preemptible: 2
        disks: "local-disk 10 HDD"
    }

    output{
        Array[String] tar_in_order = read_lines("tar_in_order")
        Array[String] count_in_order = read_lines("count_in_order")
    }
}

task mergeVariants{
    input{
        Array[File] h4_files
        Array[File] credsets
        String docker
        String zone
    }

    command <<<
        set -e
        # sort and unique the credsets
        cat <(zcat ~{credsets[0]}|head -n1) <(cat ~{write_lines(credsets)}|xargs -I % bash -c "zcat % |tail -n+2" |sort -T ./|uniq)|gzip > coloc.credsets.tsv.gz
        # merge h4 tables
        cat <(zcat ~{h4_files[0]}|head -n1) <(cat ~{write_lines(h4_files)}|xargs -I % bash -c "zcat % |tail -n+2" )|gzip > coloc.H4_tables.tsv.gz
    >>>

    output{
        File colocCredsets = "coloc.credsets.tsv.gz"
        File colocH4Tables = "coloc.H4_tables.tsv.gz"
    }

    runtime{
        cpu: 2
        memory: "6 GB"
        docker: "~{docker}"
        zones: "~{zone}"
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
        String zone
    }

    command <<<
        echo "~{sep='\n' colocs}" > list.txt
        mergeAllPair.R list.txt ~{h4pp_thresh} ~{cs_log10bf_thresh}  ~{probmass_threshold}
    >>>

    runtime{
        cpu: 2
        memory: "4 GB"
        docker: "~{docker}"
        zones: "~{zone}"
        preemptible: 0
        disks: "local-disk 100 HDD"
    }
    
    output{
        File colocQC = "colocQC.tsv.gz"
    }
}
