#!/bin/bash

#  chmod 700 postgresql_references/setup.sh - give owner permission to run this file

echo "- setting up postgresql"

# Create the file repository configuration:
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'

# Import the repository signing key:
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -

# Update the package lists:
sudo apt-get update

# Install the latest version of PostgreSQL.
# If you want a specific version, use 'postgresql-12' or similar instead of 'postgresql':
# sudo apt-get -y install postgresql
sudo apt-get install postgresql-client
sudo apt-get install postgresql postgresql-contrib
sudo apt-get install pgadmin3

echo "- setup completei"
