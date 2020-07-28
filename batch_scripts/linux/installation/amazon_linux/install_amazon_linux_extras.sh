#!/bin/bash
# Installs minimal amount of extra packages on Amazon Linux, so that we can run Docker and run git
# All the major dependencies will be installed within Docker containers by tcapy in a sandboxed environment

# Assumes Amazon username is tcapyuser (can change to ec2-user, which is the default name too)
# Whilst in practice you will likely use Amazon Linux on AWS/EC2, you can also install it for testing locally/onpremises
# for example on VWmare, by downloading the image from Amazon at
# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/amazon-linux-2-virtual-machine.html
#
# See https://medium.com/shehuawwal/download-and-run-amazon-linux-2-ami-locally-on-your-virtualbox-or-vmware-b554a98dcb1c
# which shows how to login locally

export AMAZON_USER=ec2-user

# Install docker
sudo amazon-linux-extras install docker
sudo service docker start
sudo usermod -a -G docker $AMAZON_USER
sudo chkconfig docker on

# Install docker-compose
sudo curl -L https://github.com/docker/compose/releases/download/1.26.2/docker-compose-$(uname -s)-$(uname -m) -o /usr/local/bin/docker-compose https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m) -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
docker-compose version

# Install git
sudo yum update
sudo yum install --yes git