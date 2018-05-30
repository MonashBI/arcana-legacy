#!/usr/bin/env bash
# Clones and builds a copy of dcm2niix

mkdir -p $HOME/modules
mkdir -p $HOME/packages
mkdir -p $HOME/downloads
PKG_DIR=$HOME/packages/mrtrix

if [ ! -d $PKG_DIR ]; then
  git clone https://github.com/MRtrix3/mrtrix3.git $PKG_DIR
  pushd $PKG_DIR
  ./configure
  ./build -nowarnings
  popd
fi


# Create modulefile
if [ ! -d $HOME/modules/mrtrix ]; then
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
fi
