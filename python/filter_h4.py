#!/usr/bin/env python3

"""
Merge & filter h4 tables that belong to qc'd colocalizations 
Author: Arto Lehisto <arto.lehisto@helsinki.fi>
"""

import sys,os
import gzip
import shlex, subprocess, time
from dataclasses import dataclass

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
        if f.read()[0:2] == gz_magicnumber:
            type="gz"
    if type=="normal":
        with open(fname,oper_type,encoding=encoding) as f:
            yield f
    elif type == "gz":
        with gzip.open(fname,oper_type,encoding=encoding) as f:
            yield f
    else:
        raise Exception("invalid file format")


def download_gcloud_file(fname,maxtries=6)->str:
    out_fname = "temporary_file_location"
    command = f"gcloud storage cp {fname} {out_fname}"
    current_try=1
    while True:
        proc = subprocess.run(shlex.split(command))
        if proc.returncode == 0:
            return out_fname
        if current_try>maxtries:
            raise Exception(f"Error downloading file {fname}. latest stderr: {proc.stderr}")
        print(f"Error in getting file, resetting access token and sleeping for 10 seconds. Stderr:{proc.stderr}")
        current_try+=1
        proc2 = subprocess.run(shlex.split("gcloud auth print-access-token"))
        new_access_token=proc2.stdout.strip()
        os.putenv("GCS_AUTH_TOKEN",new_access_token)
        time.sleep(10)


coloc_fname = sys.argv[1]
h4_urilist = sys.argv[2]
dataset_ids = sys.argv[3]
output_fname = sys.argv[4]

## load colocalization identifiers to memory

ds_ids = dataset_ids.split("-----")
dataset1 = ds_ids[0]
dataset2 = ds_ids[1]

data_set = set()

@dataclass(eq=True,frozen=True)
class ColocID:
    dataset1:str
    dataset2:str
    trait1:str
    trait2:str
    region1:str
    region2:str
    cs1:str
    cs2:str


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
        coloc_id = ColocID(
            dataset1,
            dataset2,
            trait1,
            trait2,
            region1,
            region2,
            cs1,
            cs2
        )

#load urilist into memory
with open(h4_urilist,"r",encoding="utf-8") as f:
    urilist = [a.strip() for a in f.readlines()]

## For each URI, download the file, then filter the rows, and simultaneously write to gzipped file.
with gzip.open(output_fname,"wt",encoding="utf-8") as out_f:
    wrote_header=False
    for uri in urilist:
        #download file
        print(f"Processing file {uri}")
        fname = download_gcloud_file(uri)
        with open(fname,"r",encoding="utf-8") as in_f:
            header = in_f.readline()
            hdi = {a:i for i,a in enumerate(header.strip().split("\t"))}
            if not wrote_header:
                out_f.write(header)
                wrote_header=True
            for l in in_f:
                cols = l.strip().split("\t")
                dataset1 = cols[hdi["dataset1"]]
                dataset2 = cols[hdi["dataset2"]]
                trait1 = cols[hdi["trait1"]]
                trait2 = cols[hdi["trait2"]]
                region1 = cols[hdi["region1"]]
                region2 = cols[hdi["region2"]]
                cs1 = cols[hdi["cs1"]]
                cs2 = cols[hdi["cs2"]]
                c_id = ColocID(
                    dataset1,
                    dataset2,
                    trait1,
                    trait2,
                    region1,
                    region2,
                    cs1,
                    cs2
                )
                if c_id in data_set:
                    out_f.write(l)