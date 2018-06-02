INSTALL_DIR=$HOME/xnat-docker-compose
mkdir -p $INSTALL_DIR
mkdir -p $HOME/xnat

# Clone the docker compose script
git clone https://github.com/monashbiomedicalimaging/xnat-docker-compose $INSTALL_DIR

pushd $INSTALL_DIR
# Checkout special branch with prefs initialised to dummy defaults
git checkout arcana-ci

# Download the XNAT WAR
mkdir webapps
wget --no-cookies https://bintray.com/nrgxnat/applications/download_file?file_path=xnat-web-${XNAT_VER}.war -O webapps/ROOT.war

# Bring up the server
docker-compose up -d
popd