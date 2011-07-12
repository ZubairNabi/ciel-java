#!/bin/bash
# Copyright (c) 2010 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
#
# ----
#
# Skywriting/Ciel local deployment helper script - not to be invoked manually. This
# is run by sw-deploy.sh after being copied to cluster machines. 
# Add any package installations or initial (one-off) setup tasks to this
# script.
# N.B.: This script assumes root privileges are given. Need to make sure that 
# the user indeed has them. Later, we probably want to add a check to ensure 
# this and otherwise throw an error.
#

echo "1/6 started" > ~/deploy-status

if [[ $1 == '' ]]; then
    SWROOT='/opt/skywriting'
else
    SWROOT=$1
fi

if [[ $2 == '' ]]; then
    GITUSER='mrry/ciel'
else
    GITUSER=$2
fi

# shut up dpkg
export DEBIAN_FRONTEND="noninteractive"

# install stuff
# XXX this requires root privileges, which the $SWUSER might not have!
apt-get -qq -y update 1>&2 2>/dev/null
apt-get -qq -y install git-core 1>&2 2>/dev/null

echo "2/6 installed minimal packages" > ~/deploy-status

# git checkout
git clone -q http://github.com/$2.git $1

echo "export PYTHONPATH=$SWROOT/src/python:\$PYTHONPATH" >> ~/.bashrc
echo "cd $SWROOT" >> ~/.bashrc

echo "3/6 checked out CIEL" > ~/deploy-status

cd $1
./scripts/install-deps-ubuntu.sh

echo "4/6 installed package dependencies" > ~/deploy-status

./build-all.sh

echo "5/6 built dependencies" > ~/deploy-status

mkdir -p /mnt/store/data
rm -rf /opt/skywriting/store
ln -s /mnt/store /opt/skywriting/store

echo "6/6 completed" > ~/deploy-status