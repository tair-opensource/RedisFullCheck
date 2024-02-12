#!/usr/bin/env bash

yum install -y git go gcc make readline-devel openssl-devel git-core zlib zlib-devel gcc-c++ patch readline libyaml-devel libffi-devel bzip2 autoconf automake libtool bison curl sqlite-devel
amazon-linux-extras install -y epel
amazon-linux-extras install -y redis6
