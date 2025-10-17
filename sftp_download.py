import os
import fnmatch
import pysftp
import json
import logging
import pandas as pd
from datetime import datetime


def load_config(config_file="D:\\Technical\\FileExtraction\\config\\sftp_dropbox_config.json"):
    """Load JSON configuration file."""
    with open(config_file, "r") as f:
        return json.load(f)


def connect_sftp(host: str, port: int, username: str, password: str, disable_hostkey_check: bool = True):
    """
    Establish an SFTP connection.
    """
    cnopts = pysftp.CnOpts()
    if disable_hostkey_check:
        cnopts.hostkeys = None

    try:
        sftp = pysftp.Connection(
            host=host,
            username=username,
            password=password,
            port=port,
            cnopts=cnopts,
        )
        logging.info(f"Connected successfully to SFTP server {host}:{port}")
        return sftp
    except Exception as e:
        logging.error(f"SFTP Connection Error ({host}:{port}): {e}")
        return None


def list_files_with_time(sftp, remote_dir: str, pattern: str = "*"):
    """
    List files in remote_dir with modified time.
    Returns [(filename, st_mtime), ...].
    """
    try:
        sftp.cwd(remote_dir)
        files = sftp.listdir_attr()
        matching_files = [f for f in files if fnmatch.fnmatch(f.filename, pattern)]
        if not matching_files:
            logging.warning(f"No files found in {remote_dir} matching {pattern}")
            return []
        return [(f.filename, f.st_mtime) for f in matching_files]
    except Exception as e:
        logging.error(f"Error listing files: {e}")
        return []


def _set_mtime(path: str, epoch: float):
    """Apply remote mtime to the local file."""
    try:
        if epoch:
            os.utime(path, (epoch, epoch))
    except Exception as e:
        logging.warning(f"Failed to set mtime on {path}: {e}")


def post_process_file(local_path, cfg, remote_mtime=None):
    """
    Handle renaming and format conversion after download, preserving timestamps.
    """
    source_ext = (cfg.get("source_ext") or "").lower()
    target_ext = (cfg.get("target_ext") or "").lower()
    target_file_name = cfg.get("target_file_name")

    local_dir = cfg["local_dir"]
    if target_file_name:
        target_path = os.path.join(local_dir, target_file_name)
    else:
        base_name = os.path.splitext(os.path.basename(local_path))[0]
        effective_ext = target_ext if target_ext else os.path.splitext(local_path)[1].lstrip(".").lower()
        target_path = os.path.join(local_dir, f"{base_name}.{effective_ext}")

    try:
        # Case 1: same extension → just rename if needed
        if source_ext == target_ext or not target_ext:
            if os.path.abspath(local_path) != os.path.abspath(target_path):
                if os.path.exists(target_path):
                    os.remove(target_path)
                os.replace(local_path, target_path)
                logging.info(f"Renamed {local_path} -> {target_path}")
            else:
                logging.info(f"No rename required for {local_path}")
            _set_mtime(target_path, remote_mtime)

        # Case 2: CSV → XLSX
        elif source_ext == "csv" and target_ext == "xlsx":
            try:
                df = pd.read_csv(local_path, engine="python", on_bad_lines="skip")
            except Exception:
                df = pd.read_csv(local_path, engine="python", sep="|", on_bad_lines="skip")
            df.to_excel(target_path, index=False)
            logging.info(f"Converted {local_path} -> {target_path}")
            os.remove(local_path)
            _set_mtime(target_path, remote_mtime)

        # Case 3: XLSX → CSV
        elif source_ext == "xlsx" and target_ext == "csv":
            df = pd.read_excel(local_path)
            df.to_csv(target_path, index=False)
            logging.info(f"Converted {local_path} -> {target_path}")
            os.remove(local_path)
            _set_mtime(target_path, remote_mtime)

        else:
            logging.warning(f"Unsupported conversion: {source_ext} -> {target_ext}. Keeping {local_path} as-is.")
            _set_mtime(local_path, remote_mtime)

    except Exception as e:
        logging.error(f"Post-processing failed for {local_path}: {e}")


def sftp_download(
    host: str,
    port: int,
    username: str,
    password: str,
    remote_dir: str,
    local_dir: str,
    file_pattern: str = "*",
    cfg: dict = None
):
    """
    Download the latest matching file with preserved timestamp.
    """
    sftp = connect_sftp(host, port, username, password)
    if not sftp:
        return

    try:
        files_with_time = list_files_with_time(sftp, remote_dir, file_pattern)
        if not files_with_time:
            return

        # Pick latest by modified time
        latest_file, remote_mtime = max(files_with_time, key=lambda f: f[1])

        os.makedirs(local_dir, exist_ok=True)
        remote_path = os.path.join(remote_dir, latest_file)
        local_path = os.path.join(local_dir, latest_file)

        # Download with timestamp preserved
        sftp.get(remote_path, local_path, preserve_mtime=True)
        logging.info(f"Downloaded latest file: {latest_file} -> {local_path}")

        # Post-process
        if cfg:
            post_process_file(local_path, cfg, remote_mtime=remote_mtime)

    finally:
        sftp.close()
        logging.info("SFTP connection closed.")


def sftp_download_by_key(src_file_name, config_file="D:\\Technical\\FileExtraction\\config\\sftp_dropbox_config.json"):
    """
    Wrapper to download latest file using just src_file_name key from config.json.
    """
    try:
        config = load_config(config_file)
        if src_file_name not in config:
            raise KeyError(f"Config for '{src_file_name}' not found in {config_file}")

        cfg = config[src_file_name]
        sftp_download(
            host=cfg["host"],
            port=cfg["port"],
            username=cfg["username"],
            password=cfg["password"],
            remote_dir=cfg["remote_dir"],
            local_dir=cfg["local_dir"],
            file_pattern=cfg.get("file_pattern", "*"),
            cfg=cfg
        )
    except Exception as e:
        logging.error(f"Error in sftp_download_by_key for {src_file_name}: {e}")
        raise
