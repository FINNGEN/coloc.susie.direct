#!/bin/bash -


dstList="$1"
saveDst="$2"
set -euo pipefail

useList=""
isFolder=false

if [[ "${dstList: -1}" == "/" ]]; then
    echo "Input is a folder: $dstList" 
    gcloud storage ls ${dstList}*.SUSIE.cred.bgz > list_temp.txt
    useList="list_temp.txt"
    isFolder=true
else
    useList="$dstList"
fi

readarray -t files < $useList
gcloud storage cp gs://finngen-production-library-green/sandbox_coloc_resources/coloc_susie/Coloc.map.txt .
echo -e "URL\ttrait\tregion"> Coloc.regions.tsv
n=${#files[@]}
for idx in "${!files[@]}"; do
    file1="${files[$idx]}"
    echo ${idx}/${n}: $file1
    url="${file1/SUSIE.cred.bgz/SUSIE.snp.bgz}"
    if [ "$isFolder" = true ]; then
        url=$(basename $url)
    fi
    gcloud storage cat $file1 | zcat | awk -v url="$url" 'BEGIN{OFS="\t"} NR>1{a[$1"\t"$2]++} END{for(b in a) print url,b}' >> Coloc.regions.tsv
done

if [ "$isFolder" = true ] && [ "$saveDst" = "save" ]; then
    gcloud storage cp Coloc.regions.tsv $dstList
    gcloud storage cp Coloc.map.txt $dstList
    echo "Saved to $dstList"
    rm Coloc.regions.tsv
    rm Coloc.map.txt
else
    echo "Use those in the colocalization"
    echo "Region: Coloc.regions.tsv"
    echo "Mapping: Coloc.map.txt"
fi

