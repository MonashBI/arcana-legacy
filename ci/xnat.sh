XNAT_VER=1.7.4

mkdir -p $HOME/packages
PKG_DIR=$HOME/packages/xnat-docker-compose


if [ ! -d $PKG_DIR ]; then
  git clone https://github.com/monashbiomedicalimaging/xnat-docker-compose $PKG_DIR
  pushd $PKG_DIR
  # Checkout special branch with prefs initialised to dummy defaults
  git checkout arcana-ci
  mkdir webapps
  wget --no-cookies https://bintray.com/nrgxnat/applications/download_file?file_path=xnat-web-${XNAT_VER}.war -O webapps/ROOT.war
  popd
fi


# Run docker-compose up
docker-compose -f $PKG_DIR/docker-compose.yml up -d