import argparse
import gzip
import os

parser = argparse.ArgumentParser("Credset files to coloc region file")
parser.add_argument("flist")
parser.add_argument("--out",default="Coloc.regions.tsv")
parser.add_argument("--url-stub",help="The prefix to URL. e.g. if files are in gs://bucket/ENDPOINT.cred.tsv.bgz, the prefix would be gs://bucket")
parser.add_argument("--url-suffix",help="The suffix after ENDPOINT in snp file URL",default=".SUSIE.snp.bgz")

args = parser.parse_args()

with open(args.flist,"rt") as f:
    csfilelist = [a.strip() for a in f.readlines()]
with open(args.out,"wt",encoding="utf-8") as outf:
    outf.write("URL\ttrait\tregion\n")
    total_regions = 0
    for csname in csfilelist:
        with gzip.open(csname,"rt",encoding="utf-8") as csf:
            header = csf.readline().strip().split("\t")
            hdi = {a:i for i,a in enumerate(header)}
            for l in csf:
                cols = l.strip().split("\t")
                endpoint=cols[hdi["trait"]]
                url = os.path.join(args.url_stub,f"{endpoint}{args.url_suffix}")
                region = cols[hdi["region"]]
                outf.write(f"{url}\t{endpoint}\t{region}\n")
                total_regions +=1
    print(f"{len(csfilelist)} files with {total_regions} regions processed to file {args.out}")
