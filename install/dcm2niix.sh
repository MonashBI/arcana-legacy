#!/usr/bin/env bash
# Clones and builds a copy of dcm2niix

PKG_DIR=$HOME/packages/dcm2niix
BUILD_DIR=$PKG_DIR/build
mkdir -p $BUILD_DIR

git clone https://github.com/rordenlab/dcm2niix.git $PKG_DIR
cd $BUILD_DIR
cmake ..
make

mkdir -p $HOME/modules/dcm2niix
echo '#%Module1.0' >> $HOME/modules/dcm2niix/1.0.2
echo 'proc ModulesHelp { } {' >> $HOME/modules/dcm2niix/1.0.2
echo 'global dotversion' >> $HOME/modules/dcm2niix/1.0.2
echo 'puts stderr "\tDcm2niix"' >> $HOME/modules/dcm2niix/1.0.2
echo '}' >> $HOME/modules/dcm2niix/1.0.2
echo 'module-whatis "Dcm2niix"' >> $HOME/modules/dcm2niix/1.0.2
echo 'conflict dcm2niix' >> $HOME/modules/dcm2niix/1.0.2
echo "prepend-path PATH $HOME/packages/dcm2niix/build/bin" >> $HOME/modules/dcm2niix/1.0.2