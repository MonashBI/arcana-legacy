#!/usr/bin/env bash
set -e

INSTALL_DIR=$HOME/xnat-docker-compose
mkdir -p $INSTALL_DIR
mkdir -p $HOME/xnat

# Clone the docker compose script
git clone https://github.com/MonashBI/xnat-docker-compose $INSTALL_DIR

pushd $INSTALL_DIR
# Checkout special branch with prefs initialised to dummy defaults
git checkout arcana-ci

# Download the XNAT WAR file and copy to webapps directory
if [ ! -f $HOME/downloads/xnat.war ]; then
  wget --no-cookies https://bintray.com/nrgxnat/applications/download_file?file_path=xnat-web-${XNAT_VER}.war -O $HOME/downloads/xnat-${XNAT_VER}.war
fi
mkdir webapps
cp $HOME/downloads/xnat-${XNAT_VER}.war webapps/ROOT.war

# Bring up the server
docker-compose up -d
popd