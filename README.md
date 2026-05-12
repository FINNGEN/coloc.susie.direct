# coloc.susie.direct
Colocalization on Susie results from FinnGen fine-mapping pipeline. This repository hosted the code only.

## Creating a new colocalization resource

Use python/create_coloc_resource.py to create the Coloc.regions.txt file (requires python 3)
How to:  
- Download all susie credible set set summary files to a folder. E.g. `data`
- make a list of all of those files. `find data/ -name "*.suffix" > filelist`
- run the script: `python3 python3/create_coloc_resource.py filelist --out Coloc.regions.txt --url-suffix "SUSIE.snp.bgz" --url-stub "gs://resource-bucket/path/"`
  - Use `--help` for more information about the script

Then, create the Coloc.map.txt file for mapping the columns. The first column is the name of the column, and hte second one is the name of the column in your dataset. For example, this is for FinnGen data:
```
trait trait
region region
rsid rsid
cs cs
low_purity low_purity
pip prob
lbf_variable_prefix lbf_variable
beta beta
se se
p p
maf maf
```
Then, upload both of those files to a bucket (e.g. to the same folder where the full susie results are), and add an entry to the coloc info file.

## Inputs
json
* colocInfo1: coloc information for paired trait1. Tab-separated with header. Required columns: `source`, `type`, `tissue`, `quant`. Plus either `url` (path prefix; script appends `/Coloc.regions.tsv` and `/Coloc.map.txt`) or `regions_file` + `mapping_file`. Use `NA` for tissue/quant when not applicable.
* colocInfo2: coloc information for paired trait2 (same format as colocInfo1)
* nColocPerBatch: number of coloc pairs distached to each VM node, default 1000
* excludeSameNameTrait: exclude the traits with the same name, default true
* h4pp\_thresh: H4 Posterior Probability threshold to merge the coloc results, default 0.5. For core analysis it is recommended that the results are further filtered with value 0.8.
* cs\_log10bf\_thresh: log10bf threshold for credible sets, default 0.9
* probmass\_threshold: Threshold for minimum probability mass in either credible set in the shared region, default 0.0. For core analysis it is recommended that the output results are further filtered to 0.9.
* docker: 
** refinery: europe-docker.pkg.dev/finngen-refinery-dev/eu.gcr.io/coloc.susie.direct:0.1.8
** sandbox: eu.gcr.io/finngen-sandbox-v3-containers/coloc.susie.direct:0.1.8

## Outputs
* pairs: Array of File,  the files contain the regions that were overlapping and that were then colocalized.
* N: Array of Int, indicates number of colocalization pairs computed. The total amount of results will be lower due to subsequent filtering.
* filtered_coloc: Array of File, containing filtered colocalization summaries for each colocalization between data sources in colocInfo1 and colocInfo2.
* hit: Array of File, coloc top signals in each pair
* unfilteredColoc: Array of File, containing the unfiltered colocalization summaries for each colocalization between data sources in colocInfo1 and data sources in colocInfo2. 
* colocCredsets: File, contains all of the credible set variants included in the filtered colocalizations.
* colocH4: File, contains all of the posterior probabilities for each variant for each colocalization in the filtered colocalizations.
* colocQC: File, filtered and merged coloc result summaries from all datasets
* unfilteredCredsets: Array of File, this contains all of the individual (per-block, so Nblocks*Nsources files) 

### Release data
For data releases, it is important to save at least the following to long-term storage:
- colocQC, which is the most important result, i.e. the filtered summary file.
- colocCredsets, which contains the credible set variants and is used in pheweb.
- unfilteredColoc. This contains the unfiltered colocalizations, so we can then find out what a colocalization that did not pass filtering looked like.
- unfiltered credible set files. This is not strictly necessary, as those credible set variants are available in both source 1 and source 2 data, but it is cumbersome to extract those after the fact.
- colocH4, which contains the H4 tables for each of the filtered colocalizations. These are not yet used anywhere, but it's good to keep them. 

## Submit to SandBox
```
# download the code
# be in the folder: cd coloc.susie.direct
# move the wdl to the script folder
mv wdl script
cd script
./submit META.json Trait Type gs://YOUR_BUCKET_can_write/demo/test_project
```

## Contacts
arto[dot]lehisto[at]helsinki[dot]fi
