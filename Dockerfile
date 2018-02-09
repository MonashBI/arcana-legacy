FROM neurodebian:xenial

RUN apt-get update; apt-get install -y git g++ python python-numpy \
    libeigen3-dev zlib1g-dev libqt4-opengl-dev libgl1-mesa-dev \
    libfftw3-dev libtiff5-dev python-pip vim wget cmake fsl-5.0

# Install Dcm2niix and MRtrix for format conversion

RUN mkdir -p /packages

# Install Dcm2niix
RUN git clone https://github.com/rordenlab/dcm2niix.git /packages/dcm2niix
RUN mkdir /packages/dcm2niix/build
WORKDIR /packages/dcm2niix/build
RUN cmake ..
RUN make

# Install MRtrix
RUN git clone https://github.com/MRtrix3/mrtrix3.git /packages/mrtrix 
WORKDIR /packages/mrtrix
RUN ./configure
RUN ./build


# FSL config
ENV FSLDIR=/usr/lib/fsl/5.0
ENV FSLOUTPUTTYPE=NIFTI_GZ
ENV PATH=$PATH:$FSLDIR
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$FSLDIR
# Run configuration script for normal usage
RUN echo ". /etc/fsl/5.0/fsl.sh" >> /root/.bashrc

# Install NiAnalysis and prerequisite pipelines
RUN pip install git+https://github.com/mbi-image/nianalysis.git
ENV PATH /packages/dcm2niix/build/bin:/packages/mrtrix/bin:$PATH
