# Install environment modules to handle package versions

mkdir -p $HOME/modules
mkdir -p $HOME/downloads

MODULE_VER=4.1.0

if [ ! -f $HOME/downloads/modules-$MODULE_VER.tar.gz ]; then 
  pushd $HOME/downloads
  wget http://downloads.sourceforge.net/project/modules/Modules/modules-$MODULE_VER/modules-$MODULE_VER.tar.gz
  tar xzf modules-$MODULE_VER.tar.gz
  popd
fi

if [ ! -d $HOME/packages/modules ]; then
  pushd $HOME/downloads/modules-$MODULE_VER
  ./configure --with-module-path=$HOME/modules --prefix=$HOME/packages/modules
  make
  make install
  echo '/modules' > $HOME/packages/modules/init/.modulespath
  cp $HOME/downloads/modules-$MODULE_VER/compat/etc/global/profile.modules $HOME/modules.sh
  sed -i 's/Modules\///g' $HOME/modules.sh
  popd
fi

# Create modulefile
if [ ! -d $HOME/modules/firstmodule ]; then
  mkdir -p $HOME/modules/firstmodule
  echo '#%Module1.0' >> $HOME/modules/firstmodule/0.16.2
  echo 'proc ModulesHelp { } {' >> $HOME/modules/firstmodule/0.16.2
  echo 'global dotversion' >> $HOME/modules/firstmodule/0.16.2
  echo 'puts stderr "\tMRtrix 0.16.2"' >> $HOME/modules/firstmodule/0.16.2
  echo '}' >> $HOME/modules/firstmodule/0.16.2
  echo 'module-whatis "MRtrix 0.16.2"' >> $HOME/modules/firstmodule/0.16.2
  echo 'conflict firstmodule' >> $HOME/modules/firstmodule/0.16.2
  echo "prepend-path PATH $HOME/packages/firstmodule/bin" >> $HOME/modules/firstmodule/0.16.2
  echo "prepend-path LD_LIBRARY_PATH $HOME/packages/firstmodule/lib" >> $HOME/modules/firstmodule/0.16.2
fi


if [ ! -d $HOME/modules/secondmodule ]; then
  mkdir -p $HOME/modules/secondmodule
  echo '#%Module1.0' >> $HOME/modules/secondmodule/1.0.2
  echo 'proc ModulesHelp { } {' >> $HOME/modules/secondmodule/1.0.2
  echo 'global dotversion' >> $HOME/modules/secondmodule/1.0.2
  echo 'puts stderr "\tDcm2niix"' >> $HOME/modules/secondmodule/1.0.2
  echo '}' >> $HOME/modules/secondmodule/1.0.2
  echo 'module-whatis "Dcm2niix"' >> $HOME/modules/secondmodule/1.0.2
  echo 'conflict secondmodule' >> $HOME/modules/secondmodule/1.0.2
  echo "prepend-path PATH $HOME/packages/secondmodule/build/bin" >> $HOME/modules/secondmodule/1.0.2
fi