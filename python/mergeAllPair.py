#!/usr/bin/env python3

"""
Streaming port of R/mergeAllPair.R.

Reads a list of gzipped per-pair coloc result TSVs, filters rows in a single
pass, and writes the surviving rows to colocQC.tsv.gz. Memory is flat (one row
at a time) — no full-table loads, no rbindlist.

Output is intended to be byte-exact (decompressed) with the R script: data
cells are passed through as raw strings; only the five numeric filter columns
are parsed, and only for comparison.
"""

import sys
import gzip
import datetime
from os.path import basename

REQUIRED = ("dataset1", "dataset2", "tissue1", "tissue2", "quant1", "quant2")
FRONT = list(REQUIRED)
NUMERIC_FILTERS = ("PP.H4.abf", "probmass_1", "probmass_2", "cs1_log10bf", "cs2_log10bf")


def log(msg):
    print(f"{datetime.datetime.now()}: {msg}")


def parse_float_or_none(s):
    try:
        return float(s)
    except ValueError:
        return None


def main():
    if len(sys.argv) != 5:
        sys.exit("usage: mergeAllPair.py <file_list> <h4_thresh> <cs_log10bf_thresh> <probmass_threshold>")

    file_list = sys.argv[1]
    h4_thresh = float(sys.argv[2])
    cs_thresh = float(sys.argv[3])
    pm_thresh = float(sys.argv[4])

    with open(file_list, "r", encoding="utf-8") as f:
        files = [ln.strip() for ln in f if ln.strip().endswith("gz")]

    n = len(files)
    out_header = None
    canonical_set = None
    total = 0

    with gzip.open("colocQC.tsv.gz", "wt", encoding="utf-8") as out:
        for idx, fp in enumerate(files, start=1):
            log(f"{idx}/{n}: {fp}")
            with gzip.open(fp, "rt", encoding="utf-8") as in_f:
                header_line = in_f.readline()
                if not header_line:
                    log(f" No rows in file: {fp}")
                    continue

                in_cols = header_line.rstrip("\n").split("\t")
                missing = [c for c in REQUIRED if c not in in_cols]
                if missing:
                    sys.exit(f"Required column(s) {missing} not found in file: {fp}")
                for c in NUMERIC_FILTERS:
                    if c not in in_cols:
                        sys.exit(f"{c} column not found in file: {fp}")

                first_row = in_f.readline()
                if not first_row:
                    log(f" No rows in file: {fp}")
                    continue

                if out_header is None:
                    rest = [c for c in in_cols if c not in FRONT]
                    out_header = FRONT + rest + ["colocRes"]
                    canonical_set = set(in_cols)
                    out.write("\t".join(out_header) + "\n")
                else:
                    if set(in_cols) != canonical_set:
                        sys.exit(f"Column-set mismatch in {fp}: expected {sorted(canonical_set)}, got {sorted(in_cols)}")

                idx_in = {c: i for i, c in enumerate(in_cols)}
                perm = tuple(-1 if c == "colocRes" else idx_in[c] for c in out_header)
                fi = tuple(idx_in[c] for c in NUMERIC_FILTERS)
                coloc_res = basename(fp)

                file_kept = 0
                file_seen = 0

                def emit(line):
                    nonlocal total, file_kept, file_seen
                    file_seen += 1
                    cols = line.rstrip("\n").split("\t")
                    h4 = parse_float_or_none(cols[fi[0]])
                    p1 = parse_float_or_none(cols[fi[1]])
                    p2 = parse_float_or_none(cols[fi[2]])
                    cs1 = parse_float_or_none(cols[fi[3]])
                    cs2 = parse_float_or_none(cols[fi[4]])
                    if h4 is None or p1 is None or p2 is None or cs1 is None or cs2 is None:
                        return
                    if not (h4 >= h4_thresh and p1 > pm_thresh and p2 > pm_thresh
                            and cs1 >= cs_thresh and cs2 >= cs_thresh):
                        return
                    out.write("\t".join(coloc_res if j == -1 else cols[j] for j in perm) + "\n")
                    file_kept += 1
                    total += 1

                emit(first_row)
                for line in in_f:
                    emit(line)

                log(f" kept {file_kept}/{file_seen} rows (running total: {total})")

    log(f"Wrote {total} rows to colocQC.tsv.gz")


if __name__ == "__main__":
    main()
