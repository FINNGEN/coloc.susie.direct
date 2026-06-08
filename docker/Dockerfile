# Minimal image for coloc.susie.direct WDL workflows (colocSusieDirectMulti, colocSusiePair).
# Two-stage build on debian:bookworm-slim.
#   Stage 1 (builder): compiles susieR + coloc from CRAN against apt-provided R deps.
#   Stage 2 (final):   runtime R + python3 + tabix + apt google-cloud-cli (gsutil included).
# Builds linux/amd64 to match GCP runtime (slow under QEMU on arm64 hosts).

ARG DEBIAN_TAG=bookworm-slim

FROM --platform=linux/amd64 debian:${DEBIAN_TAG} AS builder

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        r-base-core \
        r-base-dev \
        r-cran-data.table \
        r-cran-stringi \
        r-cran-ggplot2 \
        r-cran-matrix \
        r-cran-reshape \
        r-cran-viridis \
        r-cran-rcpp \
        r-cran-rcpparmadillo \
        r-cran-r.utils \
        build-essential \
        gfortran \
        libcurl4-openssl-dev \
        libssl-dev \
        libxml2-dev \
        zlib1g-dev \
        libbz2-dev \
        liblzma-dev \
        libdeflate-dev \
        wget \
        autoconf \
        automake \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN Rscript -e "options(Ncpus = parallel::detectCores()); \
    install.packages(c('susieR','coloc'), repos='https://cloud.r-project.org')" \
    && Rscript -e "stopifnot(requireNamespace('susieR'), requireNamespace('coloc'))"

# Build htslib from source with libcurl + GCS for gs:// URL support in tabix/bgzip.
ARG HTSLIB_VERSION=1.21.1
RUN wget -q "https://github.com/samtools/htslib/releases/download/${HTSLIB_VERSION}/htslib-${HTSLIB_VERSION}.tar.bz2" \
    && tar xjf "htslib-${HTSLIB_VERSION}.tar.bz2" \
    && cd "htslib-${HTSLIB_VERSION}" \
    && ./configure --enable-libcurl --enable-gcs --enable-s3 --prefix=/usr/local \
    && make -j2 \
    && make install \
    && cd .. && rm -rf "htslib-${HTSLIB_VERSION}" "htslib-${HTSLIB_VERSION}.tar.bz2"


FROM --platform=linux/amd64 debian:${DEBIAN_TAG} AS final

LABEL org.opencontainers.image.source="https://github.com/FINNGEN/coloc.susie.direct" \
      org.opencontainers.image.description="coloc.susie.direct runtime" \
      maintainer="juha.mehtonen@helsinki.fi"

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gnupg \
    && curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
        > /etc/apt/sources.list.d/google-cloud-sdk.list \
    && apt-get update && apt-get install -y --no-install-recommends \
        r-base-core \
        r-cran-data.table \
        r-cran-stringi \
        r-cran-ggplot2 \
        r-cran-matrix \
        r-cran-reshape \
        r-cran-viridis \
        r-cran-rcpp \
        r-cran-rcpparmadillo \
        r-cran-r.utils \
        python3 \
        libcurl4 \
        libssl3 \
        libbz2-1.0 \
        liblzma5 \
        libdeflate0 \
        google-cloud-cli \
    && apt-get purge -y curl gnupg \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/lib/R/site-library/ /usr/local/lib/R/site-library/
COPY --from=builder /usr/local/bin/tabix    /usr/local/bin/tabix
COPY --from=builder /usr/local/bin/bgzip    /usr/local/bin/bgzip
COPY --from=builder /usr/local/bin/htsfile  /usr/local/bin/htsfile
COPY --from=builder /usr/local/lib/libhts.so* /usr/local/lib/
RUN ldconfig

COPY R/*.R /usr/bin/
COPY python/*.py /usr/bin/
RUN chmod +x /usr/bin/*.R /usr/bin/*.py

WORKDIR /work
CMD ["bash"]
