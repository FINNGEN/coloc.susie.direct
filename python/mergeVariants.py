#!/usr/bin/env python3

"""
Merge & filter credset files that belong to qc'd colocalizations 
Author: Arto Lehisto <arto.lehisto@helsinki.fi>
"""

import sys
import gzip 

from contextlib import contextmanager

@contextmanager
def uopen(fname,oper_type,encoding="utf-8"):
    """
    Universal opener
    Open both gzipped and plaintext files with no fuzz
    """
    gz_magicnumber=b"\x1f\x8b"
    type="normal"
    with open(fname,"rb") as f:
        if f.read(2) == gz_magicnumber:
            type="gz"
    if type=="normal":
        with open(fname,oper_type,encoding=encoding) as f:
            yield f
    elif type == "gz":
        with gzip.open(fname,oper_type,encoding=encoding) as f:
            yield f
    else:
        raise Exception("invalid file format")

coloc_fname = sys.argv[1]
flist = sys.argv[2]
dataset_ids = sys.argv[3]
ds_ids = dataset_ids.split("-----")
dataset1 = ds_ids[0]
dataset2 = ds_ids[1]


output_fname = sys.argv[4]

data_set = set()


def str_or_none(value:str):
    if value=="NA":
        return None
    return value 

with uopen(coloc_fname,"rt",encoding="utf-8") as f:
    header = f.readline().strip().split("\t")
    hdi = {a:i for i,a in enumerate(header)}
    for line in f:
        cols = line.strip().split("\t")
        trait1 = cols[hdi["trait1"]]
        trait2 = cols[hdi["trait2"]]
        region1 = cols[hdi["region1"]]
        region2 = cols[hdi["region2"]]
        cs1 = cols[hdi["cs1"]]
        cs2 = cols[hdi["cs2"]]
        data_set.add((dataset1,trait1,region1,cs1))
        data_set.add((dataset2,trait2,region2,cs2))
        
wrote_header = False

with open(flist,"rt",encoding="utf-8") as filelistfile:
    files = [a.strip() for a in filelistfile.readlines()]
with gzip.open(output_fname,"wt",encoding="utf-8") as out_f:
    for i,fname in enumerate(files):
        print(f"{i+1}/{len(files)}: {fname}")
        with uopen(fname,"rt",encoding="utf-8") as in_f:
            header = in_f.readline()
            hdi = {a:i for i,a in enumerate(header.strip().split("\t"))}
            if not wrote_header:
                out_f.write(header)
                wrote_header = True
            for line in in_f:
                cols = line.strip().split("\t")
                dataset = str_or_none( cols[hdi["dataset"]])
                trait = str_or_none( cols[hdi["trait"]])
                region = str_or_none(cols[hdi["region"]])
                cs = str_or_none(cols[hdi["cs"]])
                if (dataset,trait,region,cs) in data_set:
                    out_f.write("\t".join(cols)+"\n")
print("Finished filtering")

