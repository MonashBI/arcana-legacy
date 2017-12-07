FROM ubuntu:16.04

RUN apt-get update; apt-get install -y git g++ python python-numpy \
                                       libeigen3-dev zlib1g-dev \
                                       libqt4-opengl-dev libgl1-mesa-dev \
                                       libfftw3-dev libtiff5-dev python-pip \
                                       vim wget

# Install MRtrix for format conversion
RUN mkdir -p /packages
RUN git clone https://github.com/MRtrix3/mrtrix3.git /packages/mrtrix 
WORKDIR /packages/mrtrix
RUN ./configure
RUN ./build

ENV PATH /packages/mrtrix/bin:$PATH

# Install NiAnalysis and prerequisite pipelines
RUN git clone --branch phantom_qc git+https://github.com/mbi-image/NiAnalysis.git
RUN pip install git+https://gitlab.erc.monash.edu.au/mbi-image/xnat-utils.git
