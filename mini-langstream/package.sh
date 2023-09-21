#!/bin/bash
version=$1
if [ -z "$version" ]; then
    echo "version is not set"
    exit 1
fi
temp_dir=$(mktemp -d)
mkdir -p $temp_dir/bin
cp mini-langstream/mini-langstream $temp_dir/bin/mini-langstream

mkdir -p target
target_filename=$(realpath target/mini-langstream-$version.zip)
rm -rf $target_filename
cd $temp_dir
zip -r $target_filename .
echo "Created $target_filename"