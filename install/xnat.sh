DOCKER_COMPOSE_VER=1.21.2
XNAT_VER=1.7.4

# Install docker-compose
curl -L https://github.com/docker/compose/releases/download/1.21.2/docker-compose-`uname -s`-`uname -m` -o /packages/docker-compose
chmod +x /packages/docker-compose

# Clone xnat-docker-compose
git clone https://github.com/monashbiomedicalimaging/xnat-docker-compose /packages/xnat-docker-compose

# Download XNAT WAR
pushd /packages/xnat-docker-compose
mkdir webapps
wget --no-cookies https://bintray.com/nrgxnat/applications/download_file?file_path=xnat-web-1.7.4.war -O webapps/xnat.war

# Run docker-compose up
sudo /packages/docker-compose -f docker-compose.yml up -d