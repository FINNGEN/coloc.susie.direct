# coloc.susie.direct
Colocalization on Susie results from Finemapping pipeline directly

## Inputs
json
* ColocSusieDirect.finemapList1: A file contains the path list to full finemap results (snp.gz)
* ColocSusieDirect.mapping1: mapping of column

* ColocSusieDirect.finemapList2:  same above, but for another coloc sets
* ColocSusieDirect.mapping2:  same above, but for another coloc sets

* ColocSusieDirect.nPerBatch: number of items to run in each instance, default 200, would be 10 - 20 minutes to run
* ColocSusieDirect.outPrefix: output name

## Outputs
* info1: region information in finemap set 1
* info2: region information in finemap set 2
* pair:  potencial colocalization pairs
* colocTotal: coloclization results
* colocHits: all the SNP information for the top coloc hits in colocTotal
