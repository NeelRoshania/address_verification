#!/bin/bash

# chmod 700 postgresql_references/setup_2.sh # give owner permission to run this file

echo "- setting up postgresql"

echo -e '\n- current services'
service --status-all # check current services
sudo apt-get install postgresql postgresql-contrib

echo -e '\n- current services'
service --status-all # check current services

echo -e '\n- list of users'
cd /
less etc/passwd | grep 'postgres'

cd ~

# start postgres service
sudo service postgresql restart
echo -e '\n- setup complete'
