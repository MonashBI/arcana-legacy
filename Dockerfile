FROM ubuntu:16.04

RUN apt-get update; apt-get install -y git g++ python python-numpy \
                                       libeigen3-dev zlib1g-dev \
                                       libqt4-opengl-dev libgl1-mesa-dev \
                                       libfftw3-dev libtiff5-dev
# Install prerequisites
RUN mkdir -p /packages
RUN git clone https://github.com/MRtrix3/mrtrix3.git /packages/mrtrix 
WORKDIR /packages/mrtrix
RUN ./configure
RUN ./build
RUN apt-get install -y python-pip
RUN pip install git+https//github.com/mbi-image/nianalysis.git@master

RUN wget https//github.com/mbi-image/nianalysis.git@master


