#!/usr/bin/env bash
# Clones and builds a copy of dcm2niix

PKG_DIR=$HOME/packages/mrtrix
mkdir -p $PKG_DIR

git clone https://github.com/MRtrix3/mrtrix3.git $PKG_DIR
cd $PKG_DIR
./configure
./build


# Create modulefile
mkdir -p $HOME/modules/mrtrix
echo '#%Module1.0' >> $HOME/modules/mrtrix/3
echo 'proc ModulesHelp { } {' >> $HOME/modules/mrtrix/3
echo 'global dotversion' >> $HOME/modules/mrtrix/3
echo 'puts stderr "\tMRtrix 3"' >> $HOME/modules/mrtrix/3
echo '}' >> $HOME/modules/mrtrix/3
echo 'module-whatis "MRtrix 3"' >> $HOME/modules/mrtrix/3
echo 'conflict mrtrix' >> $HOME/modules/mrtrix/3
echo "prepend-path PATH $HOME/packages/mrtrix/bin" >> $HOME/modules/mrtrix/3
echo "prepend-path LD_LIBRARY_PATH $HOME/packages/mrtrix/lib" >> $HOME/modules/mrtrix/3