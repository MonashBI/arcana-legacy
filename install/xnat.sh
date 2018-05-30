XNAT_VER=1.7.4

# Download XNAT WAR
mkdir -p  /packages/xnat-docker-compose
pushd /packages/xnat-docker-compose
mkdir webapps
wget --no-cookies https://bintray.com/nrgxnat/applications/download_file?file_path=xnat-web-${XNAT_VER}.war -O webapps/xnat.war

# Run docker-compose up
docker-compose -f docker-compose.yml up -d