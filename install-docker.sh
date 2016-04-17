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

cd /vagrant/docker/spark && wget http://d3kbcqa49mib13.cloudfront.net/spark-1.6.1-bin-hadoop2.6.tgz
