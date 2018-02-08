FROM ubuntu:16.04

RUN apt-get update; apt-get install -y git g++ python python-numpy \
    libeigen3-dev zlib1g-dev libqt4-opengl-dev libgl1-mesa-dev \
    libfftw3-dev libtiff5-dev python-pip vim wget cmake

# Install Dcm2niix and MRtrix for format conversion
RUN mkdir -p /packages
# Dcm2niix
RUN git clone https://github.com/rordenlab/dcm2niix.git /packages/dcm2niix
RUN mkdir /packages/dcm2niix/build
WORKDIR /packages/dcm2niix/build
RUN cmake ..
#MRtrix
RUN git clone https://github.com/MRtrix3/mrtrix3.git /packages/mrtrix 
WORKDIR /packages/mrtrix
RUN ./configure
RUN ./build


ENV PATH /packages/dcm2niix/build/bin:/packages/mrtrix/bin:$PATH

# Install NiAnalysis and prerequisite pipelines
RUN pip install git+https://github.com/mbi-image/nianalysis.git

