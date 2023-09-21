#!/bin/bash

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


echo "Installing $(tput setaf 6)mini-langstream$(tput setaf 7) please wait"

URL="https://raw.githubusercontent.com/LangStream/langstream/mini-langstream/bin/mini-langstream/mini-langstream?$(date +%s)"

app_home_dir=$HOME/.mini-langstream
mkdir -p $app_home_dir
app_bin=$app_home_dir/bin
mkdir -p $app_bin

darwin=false
case "$(uname)" in
    Darwin*)
        darwin=true
        ;;
esac

echo "$(tput setaf 2)[OK]$(tput setaf 7) - Ready to install."

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
echo "$(tput setaf 6)Downloading:$(tput setaf 7)"
curl -H 'Cache-Control: no-cache, no-store' --fail --location --progress-bar "$URL" > "$app_bin/mini-langstream"  
chmod +x $app_bin/*
echo "$(tput setaf 2)[OK]$(tput setaf 7) - Downloaded & Installed"  

function inject_if_not_found() {
    local file=$1
    touch "$file"
    if [[ -z $(grep 'mini-langstream/bin' "$file") ]]; then
        echo -e "\n$init_snipped" >> "$file"
        echo "$(tput setaf 2)[OK]$(tput setaf 7) - mini-langstream bin added to ${file}"
    fi
}




bash_profile="${HOME}/.bash_profile"
bashrc="${HOME}/.bashrc"
zshrc="${ZDOTDIR:-${HOME}}/.zshrc"
init_snipped=$( cat << EOF
export PATH=\$PATH:$app_bin
EOF
)

if [[ $darwin == true ]]; then
  inject_if_not_found $bash_profile
else
  inject_if_not_found $bashrc
fi

if [[ -s "$zshrc" ]]; then
  inject_if_not_found $zshrc
fi

echo "$(tput setaf 2)[OK]$(tput setaf 7) - Installation Successful"
echo "Open $(tput setaf 2)a new terminal$(tput setaf 7) and run: $(tput setaf 3)mini-langstream start$(tput setaf 7)"
echo ""
echo "You can close this window."