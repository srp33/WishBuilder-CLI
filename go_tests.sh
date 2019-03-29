#! /bin/bash

set -o errexit

rm -f /tmp/1*fwf* /tmp/2*fwf*
python3 Tests/ConvertMergeFilter.py
