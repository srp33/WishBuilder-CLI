#! /bin/bash

set -o errexit

tmp_dir="/tmp/WishBuilder_Parse_Tests"

mkdir -p $tmp_dir
rm -rf $tmp_dir/*

python3 UnitTest.py $tmp_dir
