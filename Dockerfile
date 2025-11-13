# Dockerfile for wind interpolation environment with R and required packages
FROM rocker/geospatial:latest

# Install Miniconda and GDAL+Arrow (with Parquet driver) via conda-forge
RUN apt-get update \
 && apt-get install -y --no-install-recommends wget bzip2 \
 && wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh \
 && bash /tmp/miniconda.sh -b -p /opt/conda \
 && rm /tmp/miniconda.sh \
 && /opt/conda/bin/conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main \
 && /opt/conda/bin/conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r \
 && /opt/conda/bin/conda install -y -c conda-forge mamba \
 && /opt/conda/bin/mamba install -y -c conda-forge \
      awscli gdal pyarrow pandas r-sf r-arrow proj proj-data \
 && /opt/conda/bin/conda clean -afy \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

ENV PATH=/opt/conda/bin:$PATH
# Ensure PROJ can find grid definitions
ENV PROJ_LIB=/opt/conda/share/proj


# RUN apt-get update && apt-get install -y --no-install-recommends \
#     cmake pkg-config \
#     libcurl4-openssl-dev libssl-dev libxml2-dev \
#     libgdal-dev libgeos-dev libproj-dev libudunits2-dev \
#     libabsl-dev \
#     && rm -rf /var/lib/apt/lists/*

# Install required R packages from CRAN
RUN R -e "install.packages(c('phylin','Metrics','dplyr','jsonlite','gstat','sp','blob','Arrow','sfarrow','reticulate','RANN'), repos='https://cloud.r-project.org/')"

# Set working directory and copy scripts
WORKDIR /app
COPY scripts/ ./scripts/

# Default working directory for script execution
WORKDIR /app/scripts

# Default entrypoint (optional)
# CMD ["bash"]
