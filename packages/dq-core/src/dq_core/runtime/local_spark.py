"""Local / standalone Apache Spark execution harness.

LOCAL HARNESS ONLY. :class:`dq_core.DataQualityEngine` never calls this module - when it runs
inline in an existing pipeline it always uses ``dataframe.sparkSession``. This module exists so
that standalone examples, the CLI and the test-suite can create and manage their own session.

Windows specifics handled here (documented in docs/spark_core_engine.md):

* Spark's ``spark-class2.cmd`` writes its launcher output to an unquoted ``%TEMP%`` path, which
  breaks when the scratch directory contains spaces. On Windows we therefore ask Spark's own
  launcher library (``org.apache.spark.launcher.Main``) for the JVM command and start it
  directly (no cmd.exe), then let PySpark attach to that gateway.
* Hadoop needs ``winutils.exe``/``hadoop.dll`` to write files on Windows. Instead of downloading
  third-party binaries we compile a tiny, auditable ``WindowsLocalFileSystem`` (source shipped in
  ``runtime/resources``) with the local JDK, unless ``HADOOP_HOME`` already provides winutils.
* All scratch space (TEMP/TMP, ``spark.local.dir``, ``java.io.tmpdir``, warehouse, derby) is
  placed under a configurable project-local directory.
"""

from __future__ import annotations

import atexit
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dq_core.runtime.errors import SparkRuntimeError
from dq_core.runtime.logging import get_logger, log_event

_RESOURCES = Path(__file__).resolve().parent / "resources"
_SHIM_CLASS = "dqaccelerator.hadoop.WindowsLocalFileSystem"
_JVM_PROCESS: subprocess.Popen[bytes] | None = None
_logger = get_logger("runtime.local_spark")


@dataclass
class LocalSparkSettings:
    """Settings for a locally managed SparkSession (standalone harness only)."""

    app_name: str = "dq-accelerator-local"
    master: str = "local[4]"
    driver_memory: str = "1g"
    shuffle_partitions: int = 4
    scratch_dir: str | None = None
    log_dir: str | None = None
    timezone: str = "UTC"
    log_level: str = "ERROR"
    extra_conf: dict[str, str] = field(default_factory=dict)

    def resolved_scratch_dir(self) -> Path:
        configured = self.scratch_dir or os.environ.get("DQ_SCRATCH_DIR")
        if configured:
            return Path(configured).resolve()
        return (Path.cwd() / ".tmp").resolve()

    def resolved_log_dir(self) -> Path:
        configured = self.log_dir or os.environ.get("DQ_LOG_DIR")
        if configured:
            return Path(configured).resolve()
        return (self.resolved_scratch_dir().parent / "logs").resolve()


def is_windows() -> bool:
    return platform.system() == "Windows"


def _prepare_scratch(settings: LocalSparkSettings) -> dict[str, Path]:
    root = settings.resolved_scratch_dir()
    dirs = {name: root / name for name in ("temp", "spark-local", "warehouse", "derby", "java-tmp", "jars")}
    for directory in dirs.values():
        directory.mkdir(parents=True, exist_ok=True)
    # Keep every temporary file (Python tempfile, Spark, JVM) on the project drive.
    os.environ["TEMP"] = os.environ["TMP"] = os.environ["TMPDIR"] = str(dirs["temp"])
    tempfile.tempdir = str(dirs["temp"])
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    return dirs


def _java_home() -> Path | None:
    java_home = os.environ.get("JAVA_HOME")
    if java_home and Path(java_home).exists():
        return Path(java_home)
    java = shutil.which("java")
    return Path(java).resolve().parent.parent if java else None


def _java_executable(name: str = "java") -> str:
    home = _java_home()
    if home is not None:
        candidate = home / "bin" / (f"{name}.exe" if is_windows() else name)
        if candidate.exists():
            return str(candidate)
    found = shutil.which(name)
    if not found:
        raise SparkRuntimeError(f"'{name}' not found. Install a JDK (17 or 21) and set JAVA_HOME.")
    return found


def _winutils_available() -> bool:
    hadoop_home = os.environ.get("HADOOP_HOME")
    return bool(hadoop_home) and (Path(hadoop_home) / "bin" / "winutils.exe").exists()


def ensure_windows_fs_shim(jar_dir: Path, spark_home: str) -> Path | None:
    """Compile the WindowsLocalFileSystem shim (once) with the local JDK. Returns the jar path."""
    jar = jar_dir / "dq-windows-localfs.jar"
    source = _RESOURCES / "WindowsLocalFileSystem.java"
    if jar.exists() and jar.stat().st_mtime >= source.stat().st_mtime:
        return jar
    try:
        javac, jar_tool = _java_executable("javac"), _java_executable("jar")
    except SparkRuntimeError:
        log_event(_logger, "windows_fs_shim_unavailable", reason="JDK compiler (javac) not found")
        return None
    classes = jar_dir / "classes"
    shutil.rmtree(classes, ignore_errors=True)
    classes.mkdir(parents=True)
    classpath = str(Path(spark_home) / "jars" / "*")
    subprocess.run([javac, "-J-XX:-UsePerfData", "--release", "17", "-nowarn", "-cp", classpath,
                    "-d", str(classes), str(source)], check=True, capture_output=True)
    subprocess.run([jar_tool, "-J-XX:-UsePerfData", "cf", str(jar), "-C", str(classes), "."],
                   check=True, capture_output=True)
    log_event(_logger, "windows_fs_shim_built", jar=str(jar))
    return jar


def _spark_conf(settings: LocalSparkSettings, dirs: dict[str, Path], shim_jar: Path | None) -> dict[str, str]:
    conf = {
        "spark.app.name": settings.app_name,
        "spark.master": settings.master,
        "spark.driver.memory": settings.driver_memory,
        "spark.driver.host": "127.0.0.1",
        "spark.driver.bindAddress": "127.0.0.1",
        "spark.ui.enabled": "false",
        "spark.ui.showConsoleProgress": "false",
        "spark.sql.shuffle.partitions": str(settings.shuffle_partitions),
        "spark.default.parallelism": str(settings.shuffle_partitions),
        "spark.sql.session.timeZone": settings.timezone,
        "spark.local.dir": str(dirs["spark-local"]),
        "spark.sql.warehouse.dir": dirs["warehouse"].as_uri(),
        "spark.pyspark.python": sys.executable,
    }
    if shim_jar is not None:
        conf["spark.driver.extraClassPath"] = str(shim_jar)
        conf["spark.hadoop.fs.file.impl"] = _SHIM_CLASS
        conf["spark.hadoop.fs.file.impl.disable.cache"] = "true"
    conf.update(settings.extra_conf)
    return conf


def _jvm_options(settings: LocalSparkSettings, dirs: dict[str, Path]) -> list[str]:
    log_dir = settings.resolved_log_dir() / "jvm"
    log_dir.mkdir(parents=True, exist_ok=True)
    return [
        f"-Djava.io.tmpdir={dirs['java-tmp']}",
        f"-Dderby.system.home={dirs['derby']}",
        f"-Duser.timezone={settings.timezone}",
        f"-Dlog4j2.configurationFile={(_RESOURCES / 'log4j2-local.properties').as_uri()}",
        "-XX:-UsePerfData",
        # JVM fatal-error dumps (hs_err_pid*.log) and heap dumps go to logs/jvm/.
        f"-XX:ErrorFile={log_dir / 'hs_err_pid%p.log'}",
        "-XX:+HeapDumpOnOutOfMemoryError",
        f"-XX:HeapDumpPath={log_dir}",
    ]


def _launch_gateway_directly(conf: dict[str, str], jvm_options: list[str], dirs: dict[str, Path]) -> None:
    """Start the Spark driver JVM without cmd.exe and expose it to PySpark via env variables."""
    global _JVM_PROCESS
    from pyspark.find_spark_home import _find_spark_home
    from pyspark.serializers import UTF8Deserializer, read_int

    spark_home = _find_spark_home()
    env = dict(os.environ, SPARK_HOME=spark_home, SPARK_SCALA_VERSION="2.13", SPARK_ENV_LOADED="1")
    launcher_cmd = [_java_executable(), "-Xmx128m", "-cp", str(Path(spark_home) / "jars" / "*"),
                    "org.apache.spark.launcher.Main", "org.apache.spark.deploy.SparkSubmit"]
    for key, value in conf.items():
        launcher_cmd += ["--conf", f"{key}={value}"]
    launcher_cmd.append("pyspark-shell")
    # SHELL makes the launcher emit a NUL-separated argv (the format spark-class uses on Unix).
    completed = subprocess.run(launcher_cmd, env=dict(env, SHELL="/bin/bash"), capture_output=True)
    if completed.returncode != 0:
        raise SparkRuntimeError(f"Spark launcher failed: {completed.stderr.decode(errors='replace')[-2000:]}")
    argv = [part.strip("\r\n") for part in completed.stdout.decode(errors="replace").split("\0")]
    argv = [part for part in argv if part]
    try:
        main_index = argv.index("org.apache.spark.deploy.SparkSubmit")
    except ValueError as exc:
        raise SparkRuntimeError(f"Unexpected Spark launcher output: {argv[:5]}") from exc
    argv[main_index:main_index] = jvm_options

    conn_dir = Path(tempfile.mkdtemp(dir=dirs["temp"]))
    conn_file = conn_dir / "gateway_conn_info"
    env["_PYSPARK_DRIVER_CONN_INFO_PATH"] = str(conn_file)
    process = subprocess.Popen(argv, stdin=subprocess.PIPE, env=env)
    deadline = time.time() + 180
    while process.poll() is None and not conn_file.exists() and time.time() < deadline:
        time.sleep(0.1)
    if not conn_file.exists():
        process.kill()
        raise SparkRuntimeError("Spark driver JVM did not start (no gateway connection info).")
    time.sleep(0.05)
    with open(conn_file, "rb") as info:
        port = read_int(info)
        secret = UTF8Deserializer().loads(info)
    shutil.rmtree(conn_dir, ignore_errors=True)
    os.environ["PYSPARK_GATEWAY_PORT"] = str(port)
    os.environ["PYSPARK_GATEWAY_SECRET"] = secret
    _JVM_PROCESS = process
    atexit.register(_terminate_jvm)


def _terminate_jvm() -> None:
    global _JVM_PROCESS
    try:  # stop Spark and close py4j connections before the JVM goes away (avoids reset noise)
        from pyspark import SparkContext

        if SparkContext._active_spark_context is not None:
            SparkContext._active_spark_context.stop()
        if SparkContext._gateway is not None:
            SparkContext._gateway.shutdown()
            SparkContext._gateway = None
            SparkContext._jvm = None
    except Exception:  # pragma: no cover - best effort shutdown
        pass
    if _JVM_PROCESS is not None and _JVM_PROCESS.poll() is None:
        try:
            if _JVM_PROCESS.stdin:
                _JVM_PROCESS.stdin.close()
            _JVM_PROCESS.wait(timeout=15)
        except Exception:  # pragma: no cover - best effort shutdown
            _JVM_PROCESS.kill()
    _JVM_PROCESS = None


def create_local_spark_session(settings: LocalSparkSettings | None = None) -> Any:
    """Create (or return the active) locally managed SparkSession."""
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    settings = settings or LocalSparkSettings()
    dirs = _prepare_scratch(settings)
    from pyspark.find_spark_home import _find_spark_home

    spark_home = _find_spark_home()
    shim_jar = ensure_windows_fs_shim(dirs["jars"], spark_home) if is_windows() and not _winutils_available() else None
    conf = _spark_conf(settings, dirs, shim_jar)
    jvm_options = _jvm_options(settings, dirs)

    from pyspark import SparkContext

    if is_windows() and SparkContext._gateway is None:
        _launch_gateway_directly(conf, jvm_options, dirs)
    elif not is_windows():
        conf["spark.driver.extraJavaOptions"] = " ".join(jvm_options)

    builder = SparkSession.builder
    for key, value in conf.items():
        builder = builder.config(key, value)
    try:
        spark = builder.getOrCreate()
    finally:
        # Never leak the gateway coordinates to child processes (they would attach to this JVM).
        os.environ.pop("PYSPARK_GATEWAY_PORT", None)
        os.environ.pop("PYSPARK_GATEWAY_SECRET", None)
    spark.sparkContext.setLogLevel(settings.log_level)
    log_event(_logger, "local_spark_session_started", spark_version=spark.version, master=settings.master,
              windows_fs_shim=bool(shim_jar), scratch_dir=str(settings.resolved_scratch_dir()))
    return spark


def stop_local_spark_session(spark: Any) -> None:
    """Stop a session created by :func:`create_local_spark_session` (JVM is reused until exit)."""
    if spark is not None:
        spark.stop()
