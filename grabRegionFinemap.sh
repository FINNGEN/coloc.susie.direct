#!/bin/bash -

set -euo pipefail
output="$1"
name="$2"
datatype="$3"
outDir="$4"


echo "This script gathers the information from finemapping pipeline to the colocalization pipeline."
echo "Maintainer: Zhili"

echo "Getting susie credible sets..."
jq '.outputs."finemap.out_susie_cred"' $output | jq -r 'def flatten: .[] | if type == "array" then flatten else . end; if type == "array" then flatten else . end // .' > grab.TMP.list

dstList="grab.TMP.list"

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

echo "Merging the regions..."
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

gcloud storage cp Coloc.regions.tsv $outDir/
gcloud storage cp Coloc.map.txt $outDir/

output_name="${name}_${datatype}.txt"
echo -e "$name\t$datatype\t$outDir/Coloc.regions.tsv\t$outDir/Coloc.map.txt" > $output_name

gcloud storage cp $output_name $outDir/

rm Coloc.regions.tsv
rm Coloc.map.txt
rm $output_name

echo "Input the following URL to the ColocSusieDirectMulti.colocInfo1"
echo "$outDir/$output_name" 
