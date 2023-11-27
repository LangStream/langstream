"""
Facilities to manage the conversion between secure-connect-bundle (plus
additional info) and the "init string".
"""
import os
from typing import Any, Dict, Optional, Tuple
import tempfile
import shutil
import json
import base64

from zipfile import ZipFile


CONFIG_FILE_NAME = "config.json"
USED_CONFIG_KEYS_AND_DEFAULTS = {
    "caCertLocation": "./ca.crt",
    "keyLocation": "./key",
    "certLocation": "./cert",
}


def encode_str(sng):
    return base64.b64encode(sng.encode()).decode()


def decode_str(b64):
    return base64.b64decode(b64).decode()


def _encode_from_string(s: str) -> str:
    return encode_str(s)


def _encode_from_file(file_dir: str, file_title: str) -> str:
    file_path = os.path.join(file_dir, file_title)
    with open(file_path) as o_file:
        return o_file.read()


def _clean_filename(fn: str) -> str:
    if fn[:2] == "./":
        return fn[2:]
    else:
        return fn


def infer_keyspace_from_bundle(bundle_path: Optional[str]) -> Optional[str]:
    """
    Given a bundle zipfile path, try to peek at its config.json
    and extract a default keyspace name on it.
    Do not raise errors: rather, silently return None instead.
    """
    if bundle_path:
        try:
            bundle_zip = ZipFile(bundle_path)
            open_config = bundle_zip.open("config.json")
            ascii_lines = [line.decode() for line in open_config.readlines()]
            config = json.loads("".join(ascii_lines))
            return config.get("keyspace")
        except Exception:
            return None
    else:
        return None


def bundle_path_to_init_string(
    secure_bundle: str,
    keyspace: Optional[str] = None,
    token: Optional[str] = None,
    tempfile_basedir: Optional[str] = None,
) -> str:
    """
    Utility function. Make the provided bundle zip into a "init string".

    1. Unzip the provided bundle to a temp dir
    2. Read and acquire (selected) contents of the zip
    3. prepare the big json bundle description
    4. make the latter into a big "init string"
    5. delete the temporary dir
    """
    with ZipFile(secure_bundle) as zipfile:
        base_dir = tempfile_basedir if tempfile_basedir else tempfile.gettempdir()
        temp_dir = tempfile.mkdtemp(dir=base_dir)
        try:
            # read config.json and find the filenames to get
            zipfile.extract(CONFIG_FILE_NAME, path=temp_dir)
            filenames = [CONFIG_FILE_NAME]
            config = json.load(open(os.path.join(temp_dir, CONFIG_FILE_NAME)))
            filenames += [
                _clean_filename(config.get(cfg_k, default))
                for cfg_k, default in USED_CONFIG_KEYS_AND_DEFAULTS.items()
            ]
            for filename in filenames:
                zipfile.extract(filename, path=temp_dir)
            #
            bundle_data = {
                filename: _encode_from_file(temp_dir, filename)
                for filename in filenames
            }
            #
            bundle_description = {
                "bundle_data": bundle_data,
                "options": {
                    "version": "1",
                    "bundle_file_name": os.path.split(secure_bundle)[1],
                    "keyspace": keyspace,
                    "token": token,
                },
            }
            #
            init_string = encode_str(
                json.dumps(
                    bundle_description,
                    separators=(",", ":"),
                )
            )
            return init_string
        finally:
            shutil.rmtree(temp_dir)


def init_string_to_bundle_path_and_options(
    init_string: str, target_dir: str
) -> Tuple[str, Dict[str, Any]]:
    """
    Make an init string back into a usable secure-connect-bundle.
    Return
        (bundle_path, options_dict)
    where options_dict is any additional info found in the init string
    beyond the bundle contents.
    """
    bundle_description = json.loads(decode_str(init_string))
    options_dict = bundle_description.get("options", {})
    version = options_dict.get("version")
    if version == "1":
        bundle_data = bundle_description["bundle_data"]
        bundle_file_name = options_dict.get(
            "bundle_file_name", "secure-connect-bundle.zip"
        )
        bundle_filepath = os.path.join(target_dir, bundle_file_name)
        with ZipFile(bundle_filepath, "w") as bundle_zip_file:
            for f_name, f_contents in bundle_data.items():
                bundle_zip_file.writestr(f_name, data=f_contents)
        return bundle_filepath, options_dict
    else:
        raise ValueError(f"Init string has unsupported or unknown version {version}.")


def create_init_string_utility():
    # Utility command-line: converts the full bundle path (argument)
    # to an "init string" and prints it as an export line
    import sys

    if sys.argv[1:] == [] or len(sys.argv) > 4:
        cmd = sys.argv[0]
        print(f"Usage: {cmd} bundle_path [keyspace [token]]")
    else:
        bundle_path = sys.argv[1]
        keyspace = None if len(sys.argv) < 3 else sys.argv[2]
        token = None if len(sys.argv) < 4 else sys.argv[3]
        print(f'- Input bundle path: "{bundle_path}".')
        print(f"- Keyspace={'Y' if keyspace else 'N'}.")
        print(f"- Token={'Y' if token else 'N'}.")
        this_init_string = bundle_path_to_init_string(bundle_path, keyspace, token)
        print("=> Export command:")
        print(f'export ASTRA_DB_INIT_STRING="{this_init_string}"')


if __name__ == "__main__":
    create_init_string_utility()
