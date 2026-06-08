#!/usr/bin/env python3

"""
Merge & filter h4 tables that belong to qc'd colocalizations
Author: Arto Lehisto <arto.lehisto@helsinki.fi>
"""

import sys,os
import gzip
import shlex, subprocess, time, datetime, tempfile, shutil
from concurrent.futures import ProcessPoolExecutor, as_completed
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

def log(msg):
    print(f"{datetime.datetime.now()}: {msg}")

def download_gcloud_file(fname,out_fname,maxtries=6)->str:
    command = f"gcloud storage cp {fname} {out_fname}"
    current_try=1
    while True:
        proc = subprocess.run(shlex.split(command), capture_output=True,encoding="utf-8")
        if proc.returncode == 0:
            return out_fname
        if current_try>maxtries:
            raise Exception(f"Error downloading file {fname}. latest stderr: {proc.stderr}")
        log(f"Error in getting file, resetting access token and sleeping for 10 seconds. Stderr:{proc.stderr}")
        current_try+=1
        proc2 = subprocess.run(shlex.split("gcloud auth print-access-token"), capture_output=True,encoding="utf-8")
        new_access_token=proc2.stdout.strip()
        os.environ["GCS_AUTH_TOKEN"]=new_access_token
        time.sleep(10)


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


## Worker-process globals, populated once per worker by _worker_init via the
## ProcessPoolExecutor initializer (so the coloc-id set is pickled once per
## worker, not once per file).
_data_set = None
_urilist = None
_tmpdir = None

def _worker_init(data_set, urilist, tmpdir):
    global _data_set, _urilist, _tmpdir
    _data_set, _urilist, _tmpdir = data_set, urilist, tmpdir

def process_uri(idx):
    """
    Runs in a worker process: download urilist[idx], filter its rows against the
    coloc-id set, and write the matching rows (no header) to a gzip shard.
    Returns counts + the header + the shard path so the parent can assemble the
    final file in URI order. CPU-heavy parsing/decompression happens here, in
    parallel across processes (threads would be GIL-bound).
    """
    uri = _urilist[idx]
    dest = os.path.join(_tmpdir, f"h4_in_{idx}")
    download_gcloud_file(uri, dest)
    shard = os.path.join(_tmpdir, f"h4_out_{idx}.tsv.gz")
    filtered_lines = 0
    unfiltered_lines = 0
    with uopen(dest,"rt",encoding="utf-8") as in_f, \
         gzip.open(shard,"wt",encoding="utf-8") as shard_f:
        header = in_f.readline()
        hdi = {a:i for i,a in enumerate(header.strip().split("\t"))}
        for l in in_f:
            cols = l.strip().split("\t")
            c_id = ColocID(
                cols[hdi["dataset1"]],
                cols[hdi["dataset2"]],
                cols[hdi["trait1"]],
                cols[hdi["trait2"]],
                cols[hdi["region1"]],
                cols[hdi["region2"]],
                cols[hdi["cs1"]],
                cols[hdi["cs2"]],
            )
            if c_id in _data_set:
                shard_f.write(l)
                filtered_lines += 1
            unfiltered_lines += 1
    os.remove(dest)  # free disk as soon as this file is filtered
    return (idx, uri, filtered_lines, unfiltered_lines, header, shard)


def main():
    coloc_fname = sys.argv[1]
    h4_urilist = sys.argv[2]
    dataset_ids = sys.argv[3]
    output_fname = sys.argv[4]
    workers = int(sys.argv[5]) if len(sys.argv) > 5 else 8

    ## load colocalization identifiers to memory
    ds_ids = dataset_ids.split("-----")
    dataset1 = ds_ids[0].split("--")[0]
    dataset2 = ds_ids[1].split("--")[0]

    data_set = set()
    with uopen(coloc_fname,"rt",encoding="utf-8") as f:
        header = f.readline().strip().split("\t")
        hdi = {a:i for i,a in enumerate(header)}
        for line in f:
            cols = line.strip().split("\t")
            data_set.add(ColocID(
                dataset1,
                dataset2,
                cols[hdi["trait1"]],
                cols[hdi["trait2"]],
                cols[hdi["region1"]],
                cols[hdi["region2"]],
                cols[hdi["cs1"]],
                cols[hdi["cs2"]],
            ))

    # load urilist into memory
    with open(h4_urilist,"r",encoding="utf-8") as f:
        urilist = [a.strip() for a in f.readlines()]

    # Download + filter every file in parallel worker processes, each writing a
    # gzip shard. The parent then concatenates shards in URI order (deterministic
    # output) after writing the header once. gzip members concatenate into a
    # valid gzip stream, so this avoids shipping filtered rows back over IPC.
    # Scratch dir for downloaded inputs + gzip shards. Default to cwd (= the
    # mounted disk under Cromwell, where PWD is the task dir) instead of /tmp,
    # which may be a small boot disk. Override with FILTER_H4_TMPDIR.
    tmp_base = os.environ.get("FILTER_H4_TMPDIR") or os.getcwd()
    tmpdir = tempfile.mkdtemp(prefix="filter_h4_", dir=tmp_base)
    log(f"Processing {len(urilist)} files with {workers} parallel workers")

    total_lines = 0
    total_lines_unfiltered = 0
    results = [None]*len(urilist)  # idx -> (header, shard_path)
    try:
        with ProcessPoolExecutor(max_workers=workers,
                                 initializer=_worker_init,
                                 initargs=(data_set, urilist, tmpdir)) as pool:
            futs = {pool.submit(process_uri, i): i for i in range(len(urilist))}
            for fut in as_completed(futs):
                idx, uri, filtered_lines, unfiltered_lines, header, shard = fut.result()
                log(f"wrote {filtered_lines} lines from file {uri}")
                results[idx] = (header, shard)
                total_lines += filtered_lines
                total_lines_unfiltered += unfiltered_lines

        # write the header once (as its own gzip member) ...
        with gzip.open(output_fname,"wt",encoding="utf-8") as out_f:
            for r in results:
                if r is not None:
                    out_f.write(r[0])
                    break
        # ... then append each shard's bytes in URI order.
        with open(output_fname,"ab") as out_b:
            for r in results:
                with open(r[1],"rb") as s:
                    shutil.copyfileobj(s, out_b)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    log(f"wrote {total_lines} in total from {len(urilist)} files")
    if total_lines_unfiltered > 0:
        log(f"In total wrote {total_lines}/{total_lines_unfiltered} lines, filtering ratio {100*total_lines/total_lines_unfiltered:0.2g}%")
    else:
        log(f"In total wrote {total_lines}/{total_lines_unfiltered} lines, no unfiltered lines to compute ratio")


if __name__ == "__main__":
    main()
