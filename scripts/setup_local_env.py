"""One-time local setup so PySpark + Delta Lake can run outside Databricks.

Downloads the Delta Lake jars (matching the delta-spark pip package) into
~/tools/jars, and on Windows also fetches winutils.exe/hadoop.dll into
~/tools/hadoop/bin (required by Hadoop's local filesystem shim). Safe to
re-run - it skips anything already present.

Usage:
    python scripts/setup_local_env.py

After it finishes, follow the printed instructions to set JAVA_HOME /
HADOOP_HOME for your shell (a JDK itself is NOT downloaded by this script -
install Temurin/OpenJDK 17 separately if `java -version` doesn't work).
"""

import os
import platform
import urllib.request

HOME = os.path.expanduser("~")
JARS_DIR = os.path.join(HOME, "tools", "jars")
HADOOP_BIN_DIR = os.path.join(HOME, "tools", "hadoop", "bin")

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


def _download(url: str, dest: str) -> None:
    if os.path.exists(dest):
        print(f"  already present: {dest}")
        return
    print(f"  downloading {url} -> {dest}")
    urllib.request.urlretrieve(url, dest)


def main() -> None:
    os.makedirs(JARS_DIR, exist_ok=True)
    print("Delta Lake jars:")
    for filename, url in DELTA_JARS.items():
        _download(url, os.path.join(JARS_DIR, filename))

    if platform.system() == "Windows":
        os.makedirs(HADOOP_BIN_DIR, exist_ok=True)
        print("winutils (Windows Hadoop shim):")
        for filename, url in WINUTILS_FILES.items():
            _download(url, os.path.join(HADOOP_BIN_DIR, filename))
        print(
            "\nSet these before running the pipeline (adjust JAVA_HOME to your JDK 17 install):\n"
            '  export JAVA_HOME="/c/path/to/jdk-17"\n'
            f'  export HADOOP_HOME="{os.path.join(HOME, "tools", "hadoop").replace(os.sep, "/")}"\n'
            '  export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH"\n'
        )
    else:
        print("\nNon-Windows OS detected: no winutils needed. Just ensure JAVA_HOME points at a JDK 17 install.")

    print('Done. Verify with: java -version && python -c "import pyspark, delta"')


if __name__ == "__main__":
    main()
