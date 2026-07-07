"""One-time local setup so PySpark + Delta Lake can run outside Databricks.

Downloads the Delta Lake jars (matching the delta-spark pip package) into
~/tools/jars, and on Windows also fetches winutils.exe/hadoop.dll into
~/tools/hadoop/bin (required by Hadoop's local filesystem shim). If no
working `java` is found on PATH/JAVA_HOME, also downloads a portable
Temurin 17 JDK into ~/tools/jdk-17 - nothing is installed system-wide.

Safe to re-run: every step first checks whether it's already satisfied
(an existing JAVA_HOME that works, a jar/binary already on disk) and skips
it. If you already have a working JDK, this script never touches it or
downloads another one.

Usage:
    python scripts/setup_local_env.py
"""

import os
import platform
import shutil
import subprocess
import urllib.request
import zipfile

HOME = os.path.expanduser("~")
JARS_DIR = os.path.join(HOME, "tools", "jars")
HADOOP_BIN_DIR = os.path.join(HOME, "tools", "hadoop", "bin")
JDK_DIR = os.path.join(HOME, "tools", "jdk-17")

DELTA_JARS = {
    "delta-spark_2.12-3.2.0.jar": (
        "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar"
    ),
    "delta-storage-3.2.0.jar": ("https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar"),
}

WINUTILS_FILES = {
    "winutils.exe": "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/winutils.exe",
    "hadoop.dll": "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/hadoop.dll",
}

# Portable (zip, no installer) Temurin 17 builds - only used if java isn't found.
PORTABLE_JDK_URLS = {
    "Windows": "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.19%2B10/"
    "OpenJDK17U-jdk_x64_windows_hotspot_17.0.19_10.zip",
    "Linux": "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.19%2B10/"
    "OpenJDK17U-jdk_x64_linux_hotspot_17.0.19_10.tar.gz",
    "Darwin": "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.19%2B10/"
    "OpenJDK17U-jdk_x64_mac_hotspot_17.0.19_10.tar.gz",
}


def _download(url: str, dest: str) -> None:
    if os.path.exists(dest):
        print(f"  already present: {dest}")
        return
    print(f"  downloading {url} -> {dest}")
    urllib.request.urlretrieve(url, dest)


def _java_home_that_works() -> str | None:
    """Returns an existing JAVA_HOME if `java -version` already works with
    it, so we never re-download a JDK you already have configured."""
    candidates = []
    if os.environ.get("JAVA_HOME"):
        candidates.append(os.environ["JAVA_HOME"])
    java_on_path = shutil.which("java")
    if java_on_path:
        # .../<JAVA_HOME>/bin/java(.exe)
        candidates.append(os.path.dirname(os.path.dirname(java_on_path)))

    for candidate in candidates:
        java_bin = os.path.join(candidate, "bin", "java.exe" if platform.system() == "Windows" else "java")
        if os.path.exists(java_bin):
            try:
                subprocess.run([java_bin, "-version"], capture_output=True, check=True)
                return candidate
            except (subprocess.CalledProcessError, OSError):
                continue
    return None


def _ensure_jdk() -> str:
    existing = _java_home_that_works()
    if existing:
        print(f"Java: found a working JDK at {existing} - skipping download.")
        return existing

    system = platform.system()
    url = PORTABLE_JDK_URLS.get(system)
    if not url:
        raise RuntimeError(f"No portable JDK mapping for {system}; install a JDK 17 manually.")

    print(f"Java: no working JDK found, downloading a portable Temurin 17 for {system}...")
    os.makedirs(JDK_DIR, exist_ok=True)
    archive_path = os.path.join(JDK_DIR, os.path.basename(url).split("?")[0])
    _download(url, archive_path)

    if archive_path.endswith(".zip"):
        with zipfile.ZipFile(archive_path) as zf:
            zf.extractall(JDK_DIR)
    else:
        shutil.unpack_archive(archive_path, JDK_DIR)

    # The archive contains one top-level "jdk-17.x+y" folder - surface it directly.
    extracted = [d for d in os.listdir(JDK_DIR) if d.lower().startswith("jdk-")]
    return os.path.join(JDK_DIR, extracted[0]) if extracted else JDK_DIR


def main() -> None:
    os.makedirs(JARS_DIR, exist_ok=True)
    print("Delta Lake jars:")
    for filename, url in DELTA_JARS.items():
        _download(url, os.path.join(JARS_DIR, filename))

    java_home = _ensure_jdk()

    hadoop_home = None
    if platform.system() == "Windows":
        os.makedirs(HADOOP_BIN_DIR, exist_ok=True)
        print("winutils (Windows Hadoop shim):")
        for filename, url in WINUTILS_FILES.items():
            _download(url, os.path.join(HADOOP_BIN_DIR, filename))
        hadoop_home = os.path.join(HOME, "tools", "hadoop").replace(os.sep, "/")
    else:
        print("Non-Windows OS detected: no winutils needed.")

    print("\nExport these before running the pipeline:")
    print(f'  export JAVA_HOME="{java_home.replace(os.sep, "/")}"')
    if hadoop_home:
        print(f'  export HADOOP_HOME="{hadoop_home}"')
        print('  export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH"')
    else:
        print('  export PATH="$JAVA_HOME/bin:$PATH"')

    print('\nDone. Verify with: java -version && python -c "import pyspark, delta"')


if __name__ == "__main__":
    main()
