# Install environment modules to handle package versions

set -e

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
mkdir -p $HOME/modules/firsttestmodule
echo '#%Module1.0' >> $HOME/modules/firsttestmodule/0.16.2
echo 'proc ModulesHelp { } {' >> $HOME/modules/firsttestmodule/0.16.2
echo 'global dotversion' >> $HOME/modules/firsttestmodule/0.16.2
echo 'puts stderr "\tFirst Test Module 0.16.2"' >> $HOME/modules/firsttestmodule/0.16.2
echo '}' >> $HOME/modules/firsttestmodule/0.16.2
echo 'module-whatis "First Test Module 0.16.2"' >> $HOME/modules/firsttestmodule/0.16.2
echo 'conflict firsttestmodule' >> $HOME/modules/firsttestmodule/0.16.2
echo "prepend-path PATH $HOME/packages/firsttestmodule/bin" >> $HOME/modules/firsttestmodule/0.16.2
echo "prepend-path LD_LIBRARY_PATH $HOME/packages/firsttestmodule/lib" >> $HOME/modules/firsttestmodule/0.16.2
echo "setenv FIRSTTESTMODULE_VERSION 0.16.2.dev18" >> $HOME/modules/firsttestmodule/0.16.2


mkdir -p $HOME/modules/secondtestmodule
echo '#%Module1.0' >> $HOME/modules/secondtestmodule/1.0.2
echo 'proc ModulesHelp { } {' >> $HOME/modules/secondtestmodule/1.0.2
echo 'global dotversion' >> $HOME/modules/secondtestmodule/1.0.2
echo 'puts stderr "\tSecond Test Module"' >> $HOME/modules/secondtestmodule/1.0.2
echo '}' >> $HOME/modules/secondtestmodule/1.0.2
echo 'module-whatis "Second Test Module"' >> $HOME/modules/secondtestmodule/1.0.2
echo 'conflict secondtestmodule' >> $HOME/modules/secondtestmodule/1.0.2
echo "prepend-path PATH $HOME/packages/secondtestmodule/build/bin" >> $HOME/modules/secondtestmodule/1.0.2
echo "setenv SECONDTESTMODULE_VERSION 1.0.3a2" >> $HOME/modules/secondtestmodule/1.0.2
