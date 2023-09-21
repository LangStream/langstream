#!/bin/bash
#
# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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