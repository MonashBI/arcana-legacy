# Install environment modules to handle package versions

mkdir -p $HOME/modules
mkdir -p $HOME/packages
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
