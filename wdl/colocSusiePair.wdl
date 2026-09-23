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
        String zone
    }

    Int block = ceil(1.0 * N / nColocPerBatch)
    scatter(blk in range(block)){
       call coloc{input: colocInfo=info, nPerBatch=nColocPerBatch, block=blk, docker=docker,zone=zone}
    }

    call mergeColoc{input: colocs=coloc.res, hits=coloc.hits, credsets=coloc.credset_variants, out_stub=basename(info,".pairs.tar.gz"), docker=docker,
        h4pp_thresh=h4pp_thresh,cs_log10bf_thresh=cs_log10bf_thresh,probmass_threshold=probmass_threshold,zone=zone}

    call mergeH4Tables{
        input: colocInfo=mergeColoc.coloc_out,h4_variant_tables=coloc.h4_variants,docker=docker,zone=zone
    }

    output{
        File unfiltered_sum = mergeColoc.unfiltered
        File sum = mergeColoc.coloc_out
        File hit = mergeColoc.hit
        File h4_variant = mergeH4Tables.h4_variant
        File credset = mergeColoc.credset
        Array[File] unfiltered_credsets = coloc.credset_variants
    }

    meta{
        version: "0.1.9"
    }
}


task coloc{
    input{
        File colocInfo
        Int nPerBatch
        Int block
        String docker
        String zone
    }

    command <<<
        filename=$(basename "~{colocInfo}")
        dataset_id="$(echo $filename | sed 's/.pairs.tar.gz//')"
        #this is needed in the coloc.R command, since it has commands consisting of piped commands, which do not show correct return code without pipefail
        set -o pipefail
        tar xvf ~{colocInfo}
        coloc.R pairs.tsv map1.txt map2.txt ~{nPerBatch} ~{block} "${dataset_id}"
        #save 
        
        #make sure credset vars are unique. If it doesn't exist, create empty file
        if [ ! -f "region~{block}.credsets.tsv" ]; then
            touch "region~{block}.credsets.tsv.gz"
        else
            cat <(head -n1 "region~{block}.credsets.tsv") <(tail -n+2 "region~{block}.credsets.tsv"|sort|uniq)|gzip > "region~{block}.credsets.tsv.gz"
        fi

        if [ ! -f "region~{block}.h4_variants.tsv.gz" ]; then
            touch "region~{block}.h4_variants.tsv.gz"
        fi
    >>>

    runtime{
        cpu: 2
        memory: "4 GB"
        docker: "~{docker}"
        zones: "~{zone}"
        disks: "local-disk 100 HDD"
        noAddress: true
        maxRetries: 2
    }

    output{
        File res = "region" + block + ".sum.tsv"
        File hits = "region" + block + ".hits.tsv"
        File credset_variants = "region" + block + ".credsets.tsv.gz"
        File h4_variants = "region" + block + ".h4_variants.tsv.gz"
    }
}


task mergeH4Tables{
    input{
        File colocInfo
        Array[String] h4_variant_tables
        String docker
        String zone
    }
    String out_stub=basename(colocInfo,".sum.tsv.gz")
    Int n_tables = length(h4_variant_tables)
    Int cpus = if n_tables < 4 then 1 else if n_tables < 20 then 2 else if n_tables < 40 then 4 else 8
    Int workers = if n_tables == 1 then 1 else cpus * 2
    command <<<
        set -e
        #set gcloud auth 
        date
        echo "combining H4 tables"
        export GCS_AUTH_TOKEN="$(gcloud auth print-access-token)"
        filter_h4.py ~{colocInfo} ~{write_lines(h4_variant_tables)} "~{out_stub}" "~{out_stub}.h4.variants.tsv.gz" ~{workers}
    >>>

    runtime{
        cpu: cpus
        memory: "16 GB"
        docker: "~{docker}"
        noAddress: true
        zones: "~{zone}"
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
        String out_stub
        String docker
        String zone
        Float h4pp_thresh
        Float cs_log10bf_thresh
        Float probmass_threshold
    }
    command <<<
        echo "~{sep='\n' colocs}" > sum.txt
        echo "~{sep='\n' hits}" > hits.txt
        echo "~{sep='\n' credsets}" > credsets.txt

        mkdir sums h4_variants hits credsets

        is_nonempty() {
            local f="$1"
            [ -s "$f" ] || return 1
            local nlines
            case "$f" in
                *.gz) nlines=$(zcat "$f" 2>/dev/null | head -n 2 | wc -l) ;;
                *)    nlines=$(head -n 2 "$f" | wc -l) ;;
            esac
            [ "$nlines" -ge 2 ]
        }

        filter_nonempty() {
            local in_list="$1"
            local out_list="$2"
            : > "$out_list"
            while IFS= read -r f; do
                [ -z "$f" ] && continue
                if is_nonempty "$f"; then
                    echo "$f" >> "$out_list"
                fi
            done < "$in_list"
        }

        date
        echo "combining sumstats"
        cat sum.txt | gcloud storage cp -I sums/
        find sums/ -name "*.sum.tsv" > sum_list
        filter_nonempty sum_list sum_list.nonempty
        if [ -s sum_list.nonempty ]; then
            filterSums.R sum_list.nonempty ~{h4pp_thresh} ~{cs_log10bf_thresh} ~{probmass_threshold} "~{out_stub}.sum.tsv.gz"
            awk 'FNR>1 || NR==1' $(cat sum_list.nonempty) | gzip > ~{out_stub}.sum.unfiltered.tsv.gz
        else
            echo "no non-empty sum files; emitting empty outputs"
            touch "~{out_stub}.sum.tsv.gz" "~{out_stub}.sum.unfiltered.tsv.gz"
        fi
        rm -f sums/*.tsv

        date
        echo "combining hit files"
        cat hits.txt | gcloud storage cp -I hits/
        find hits/ -name "*.tsv" > hits_list
        filter_nonempty hits_list hits_list.nonempty
        if [ -s hits_list.nonempty ]; then
            awk 'FNR>1 || NR==1' $(cat hits_list.nonempty) | gzip > ~{out_stub}.hits.tsv.gz
        else
            echo "no non-empty hits files; emitting empty output"
            touch "~{out_stub}.hits.tsv.gz"
        fi
        rm -f hits/*.tsv

        date
        echo "combining credset files"
        cat credsets.txt | gcloud storage cp -I credsets/
        find credsets/ -name "*.credsets.tsv.gz" > cs_list
        filter_nonempty cs_list cs_list.nonempty
        if [ -s cs_list.nonempty ]; then
            if [ -s "~{out_stub}.sum.tsv.gz" ]; then
                mergeVariants.py "~{out_stub}.sum.tsv.gz" cs_list.nonempty "~{out_stub}" "temp_cs.gz"
                cat <(zcat "temp_cs.gz" | head -n1) <(zcat "temp_cs.gz" | tail -n+2 | sort -T ./ | uniq) | gzip > "~{out_stub}.credset.tsv.gz"
            else
                echo "sum output empty; emitting empty filtered credset"
                touch "~{out_stub}.credset.tsv.gz"
            fi
            date
            echo "combining unfiltered credset files"
            first_cs=$(head -n1 cs_list.nonempty)
            cat <(zcat "$first_cs" | head -n1) <(cat cs_list.nonempty | xargs -I % bash -c 'zcat % | tail -n +2 ' | sort -T ./ | uniq) | gzip > ~{out_stub}.credset.unfiltered.tsv.gz
        else
            echo "no non-empty credset files; emitting empty outputs"
            touch "~{out_stub}.credset.tsv.gz" "~{out_stub}.credset.unfiltered.tsv.gz"
        fi
    >>>

    runtime{
        cpu: 1
        memory: "4 GB"
        docker: "~{docker}"
        noAddress: true
        zones: "~{zone}"
        disks: "local-disk 500 HDD"
        preemptible: 0
    }

    output{
        File coloc_out = out_stub+".sum.tsv.gz"
        File hit = out_stub + ".hits.tsv.gz"
        File credset = out_stub+".credset.tsv.gz"
        File unfiltered = out_stub+".sum.unfiltered.tsv.gz"
        File unfiltered_cs = out_stub+".credset.unfiltered.tsv.gz"
    }
}
