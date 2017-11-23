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

# Install NiAnalysis and prerequisite pipelines
RUN pip install git+https://github.com/mbi-image/NiAnalysis.git@phantom_qc

# Add docker user
RUN useradd -ms /bin/bash docker
USER docker
ENV HOME=/home/docker
WORKDIR $HOME

# Add netrc
COPY .netrc $HOME/.netrc

# Download QA script to run
RUN mkdir $HOME/scripts
RUN wget https://raw.githubusercontent.com/mbi-image/NiAnalysis/phantom_qc/scripts/qa.py $HOME/scripts
