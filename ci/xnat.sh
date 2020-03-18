#!/usr/bin/env bash
set -e

mkdir -p $XNAT_DIR

# Clone the docker compose script
git clone https://github.com/MonashBI/xnat-docker-compose $XNAT_DIR

cp ci/prefs-init.ini $XNAT_DIR/
cp ci/xnat-docker-compose.prefs.yml $XNAT_DIR/docker-compose.prefs.yml

pushd $XNAT_DIR

# Download the XNAT WAR file and copy to webapps directory
if [ ! -f $HOME/downloads/xnat.war ]; then
	wget --no-cookies https://api.bitbucket.org/2.0/repositories/xnatdev/xnat-web/downloads/xnat-web-${XNAT_VER}.war -O $HOME/downloads/xnat-${XNAT_VER}.war
fi
mkdir webapps
cp $HOME/downloads/xnat-${XNAT_VER}.war webapps/ROOT.war

popd
