#!/bin/bash
#
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

set -e

track_last_command() {
    last_command=$current_command
    current_command=$BASH_COMMAND
}
trap track_last_command DEBUG

echo_failed_command() {
    local exit_code="$?"
	if [[ "$exit_code" != "0" ]]; then
		echo "'$last_command': command failed with exit code $exit_code."
	fi
}

trap echo_failed_command EXIT

clear


echo "  _                      ____  _                            ";
echo " | |    __ _ _ __   __ _/ ___|| |_ _ __ ___  __ _ _ __ ___  ";
echo " | |   / _\` | '_ \ / _\` \___ \| __| '__/ _ \/ _\` | '_ \` _ \ ";
echo " | |__| (_| | | | | (_| |___) | |_| | |  __/ (_| | | | | | |";
echo " |_____\__,_|_| |_|\__, |____/ \__|_|  \___|\__,_|_| |_| |_|";
echo "                   |___/                                    ";


echo "Installing $(tput setaf 6)LangStream CLI$(tput setaf 7) please wait...      "
# Local installation
BIN_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ROOT_DIR=$( cd -P "$( dirname "$BIN_DIR" )" >/dev/null 2>&1 && pwd )
DEFAULT_ZIP="https://github.com/LangStream/langstream/releases/download/0.0.2-alpha/langstream-cli-0.0.2-alpha.zip"
ZIP_DOWNLOAD_URL="${1:-$DEFAULT_ZIP}"
candidate_base_name=$(basename $ZIP_DOWNLOAD_URL)


langstream_root_dir="$HOME/.langstream"
mkdir -p $langstream_root_dir
langstream_downloads_dir="$langstream_root_dir/downloads"
mkdir -p $langstream_downloads_dir
langstream_candidates_dir="$langstream_root_dir/candidates"
mkdir -p $langstream_candidates_dir

langstream_current_symlink="$langstream_candidates_dir/current"
mkdir -p $langstream_candidates_dir

downloaded_zip_path=$langstream_downloads_dir/$candidate_base_name
downloaded_extracted_dir=${ZIP_DOWNLOAD_URL//\.zip}
downloaded_extracted_path="$langstream_candidates_dir/$(basename $downloaded_extracted_dir)"

darwin=false
case "$(uname)" in
    Darwin*)
        darwin=true
        ;;
esac

echo "$(tput setaf 2)[OK]$(tput setaf 7) - Ready to install $(basename $downloaded_extracted_dir)."

if ! command -v unzip > /dev/null; then
	echo "Not found."
	echo "======================================================================================================"
	echo " Please install unzip on your system using your favourite package manager."
	echo ""
	echo " Restart after installing unzip."
	echo "======================================================================================================"
	echo ""
	exit 1
fi
echo "$(tput setaf 2)[OK]$(tput setaf 7) - unzip command is available"

if ! command -v curl > /dev/null; then
	echo "Not found."
	echo ""
	echo "======================================================================================================"
	echo " Please install curl on your system using your favourite package manager."
	echo ""
	echo " Restart after installing curl."
	echo "======================================================================================================"
	echo ""
	exit 1
fi
echo "$(tput setaf 2)[OK]$(tput setaf 7) - curl command is available"

echo ""
echo "$(tput setaf 6)Downloading archive:$(tput setaf 7)"
if [ -f "$downloaded_zip_path" ]; then
	echo "$(tput setaf 2)[OK]$(tput setaf 7) - Archive is already there"
else
	curl --fail --location --progress-bar "$ZIP_DOWNLOAD_URL" > "$downloaded_zip_path"
	echo "$(tput setaf 2)[OK]$(tput setaf 7) - File downloaded"
fi

# check integrity
ARCHIVE_OK=$(unzip -qt "$downloaded_zip_path" | grep 'No errors detected in compressed data')
if [[ -z "$ARCHIVE_OK" ]]; then
	echo "Downloaded zip archive corrupt. Are you connected to the internet?"
	exit
fi
echo "$(tput setaf 2)[OK]$(tput setaf 7) - Integrity of the archive checked"

echo ""
echo "$(tput setaf 6)Extracting and installation:$(tput setaf 7)"
unzip -qo "$downloaded_zip_path" -d "$langstream_candidates_dir"
echo "$(tput setaf 2)[OK]$(tput setaf 7) - Extraction is successful"

rm -rf $langstream_current_symlink
ln -s $downloaded_extracted_path $langstream_current_symlink

echo "$(tput setaf 2)[OK]$(tput setaf 7) - LangStream CLI installed at $langstream_candidates_dir"

function inject_if_not_found() {
    local file=$1
    touch "$file"
    if [[ -z $(grep 'langstream/candidates' "$file") ]]; then
        echo -e "\n$init_snipped" >> "$file"
        echo "$(tput setaf 2)[OK]$(tput setaf 7) - langstream bin added to ${file}"
    fi
}

function inject_completions_if_not_found() {
    local file=$1
    touch "$file"
    if [[ -z $(grep 'langstream generate-completion' "$file") ]]; then
        echo -e "source <(langstream generate-completion)" >> "$file"
        echo "$(tput setaf 2)[OK]$(tput setaf 7) - langstream completions added to ${file}"
    fi
}



bash_profile="${HOME}/.bash_profile"
bashrc="${HOME}/.bashrc"
zshrc="${ZDOTDIR:-${HOME}}/.zshrc"
init_snipped=$( cat << EOF
export PATH=\$PATH:$langstream_current_symlink/bin
EOF
)

if [[ $darwin == true ]]; then
  inject_if_not_found $bash_profile
  inject_completions_if_not_found $bash_profile
else
  inject_if_not_found $bashrc
  inject_completions_if_not_found $bashrc
fi

if [[ -s "$zshrc" ]]; then
  inject_if_not_found $zshrc
  inject_completions_if_not_found $zshrc
fi

echo "$(tput setaf 2)[OK]$(tput setaf 7) - Installation Successful"
echo "Open $(tput setaf 2)a new terminal$(tput setaf 7) and run: $(tput setaf 3)langstream$(tput setaf 7)"
echo ""
echo "You can close this window."
