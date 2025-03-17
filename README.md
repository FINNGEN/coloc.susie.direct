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
* colocInfo1: coloc information for paired trait1, format: dataset name, dataset type, resource link
* colocInfo2: coloc information for paired trait2
* nColocPerBatch: number of coloc pairs distached to each VM node, default 1000
* excludeSameNameTrait: exclude the traits with the same name, default true
* h4pp\_thresh: H4 PP threshold to merge the coloc results, default 0.5
* cs\_log10bf\_thresh1: log10bf threshold for credible set in finemapped cs1:cs1 pair, default 0.9
* cs\_log10bf\_thresh2: log10bf threshold for other pair, default 1.0
* docker: 
** refinery: europe-docker.pkg.dev/finngen-refinery-dev/eu.gcr.io/coloc.susie.direct:0.1.8
** sandbox: eu.gcr.io/finngen-sandbox-v3-containers/coloc.susie.direct:0.1.8

## Outputs
* pairs: Array of File,  region matched for each dataset
* N: Array of Int, indicate number of pairs in region
* coloc: Array of File, coloc results for each pair
* hit: Array of File, coloc top signals in each pair
* colocQC: File, QC and merged coloc results from all dataset

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
zhili[dot]zheng[at]broadinstitute[dot]org
