# Install Python tools
apt-get install -y python-pip
pip install urlfetch

# Provision docker
wget -qO- https://get.docker.com/ | sh

# Fix docker permissions so they can be used w/o root
groupadd docker
gpasswd -a vagrant docker
service docker restart

# Pull the Hadoop image
docker pull sequenceiq/hadoop-docker:2.7.0
