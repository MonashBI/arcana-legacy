INSTALL_DIR=$HOME/xnat-docker-compose
mkdir -p $INSTALL_DIR
mkdir -p $HOME/xnat

# Clone the docker compose script
git clone https://github.com/MonashBI/xnat-docker-compose $INSTALL_DIR

pushd $INSTALL_DIR
# Checkout special branch with prefs initialised to dummy defaults
git checkout arcana-ci

# Download the XNAT WAR file and copy to webapps directory
if [ ! $HOME/downloads/xnat.war ]; then
  wget --no-cookies https://bintray.com/nrgxnat/applications/download_file?file_path=xnat-web-${XNAT_VER}.war -O $HOME/downloads/xnat.war
fi
mkdir webapps
cp $HOME/downloads/xnat.war webapps/ROOT.war

# Bring up the server
docker-compose up -d
popd