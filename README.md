# coloc.susie.direct
Colocalization on Susie results from FinnGen fine-mapping pipeline. This repository hosted the code only.

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
** refinery: europe-docker.pkg.dev/finngen-refinery-dev/eu.gcr.io/coloc.susie.direct:0.1.7 
** sandbox: eu.gcr.io/finngen-sandbox-v3-containers/coloc.susie.direct:0.1.7

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
