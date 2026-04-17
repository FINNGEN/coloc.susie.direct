version 1.0

workflow ColocMerge{
    input{
        File allColoc_loc
        File allH4Table_loc
        File allCredset_loc

        Float h4pp_thresh = 0.5
        Float cs_log10bf_thresh = 0.9
        Float probmass_threshold = 0.9
        String docker = "eu.gcr.io/finngen-sandbox-v3-containers/coloc.susie.direct:0.1.7"
    }

    Array[File] allColoc = read_lines(allColoc_loc)
    Array[File] allH4Table = read_lines(allH4Table_loc)
    Array[File] allCredset = read_lines(allCredset_loc)

    call mergeAllPair{
        input: colocs=allColoc, h4pp_thresh=h4pp_thresh, cs_log10bf_thresh=cs_log10bf_thresh, docker=docker,probmass_threshold=probmass_threshold
    }

    call mergeVariants{
        input: h4_files = allH4Table, credsets=allCredset,docker=docker
    }

    output{
        File colocCredsets = mergeVariants.colocCredsets
        File colocH4 = mergeVariants.colocH4Tables
        File colocQC = mergeAllPair.colocQC
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
