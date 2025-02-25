FROM zhiliz/debian12-gdk:0.0.1
LABEL maintainer="zhili.zheng@broadinstitute.org"

RUN Rscript -e "install.packages(c('coloc'))"
COPY R/*.R /usr/bin
COPY python/*.py /usr/bin
RUN chmod +x /usr/bin/*.py
RUN chmod +x /usr/bin/*.R
