# coloc.susie.direct
Colocalization on Susie results from Finemap pipeline directly

## Inputs
json
* colocInfo1: coloc information for paired trait1
* colocInfo2: coloc information for paired trait2
* nColocPerBatch: number of coloc pairs distached to each VM node, default 1000
* excludeSameNameTrait: exclude the traits with the same name, default true
* h4pp\_thresh: H4 PP threshold to merge the coloc results, default 0.5
* cs\_log10bf\_thresh1: log10bf threshold for credible set in finemapped cs1:cs1 pair
* cs\_log10bf\_thresh2: log10bf threshold for other pair

## Outputs
* pairs: Array of File,  region matched for each dataset
* N: Array of Int, indicate number of pairs in region
* coloc: Array of File, coloc results for each pair
* hit: Array of File, coloc top signals in each pair
* colocQC: File, QC and merged coloc results from all dataset

## Contacts
zhili[dot]zheng[at]broadinstitute[dot]org
