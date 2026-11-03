"""Start and stop a disposable local douchdb cluster for the harness."""

import os
import shutil
import signal
import subprocess
import tempfile
import time

import requests

import config


class Cluster:
    def __init__(self, nodes=None, binary=None):
        self.ports = list(config.PORTS)
        if nodes:
            if isinstance(nodes, int):
                self.ports = self.ports[:nodes]
            else:
                self.ports = list(nodes)
        self.binary = binary or config.BIN
        self.secret = config.SECRET
        self.root = tempfile.mkdtemp(prefix="douchdb-jepsen-")
        self.processes = {}
        self._started = False

    @property
    def urls(self):
        return [config.make_url(port) for port in self.ports]

    def _node_args(self, index):
        return [
            self.binary,
            "--listen",
            f"{config.HOST}:{self.ports[index]}",
            "--data",
            os.path.join(self.root, f"node{index}"),
            "--q",
            str(config.Q),
            "--n",
            str(len(self.ports)),
            "--nodes",
            ",".join(self.urls),
            "--me",
            str(index),
            "--sync-secs",
            config.SYNC_SECS,
            "--cluster-secret",
            self.secret,
        ]

    def start(self, timeout=30):
        if not os.path.exists(self.binary):
            raise RuntimeError(
                f"douchdb binary not found: {self.binary}; set DOUCHDB_BIN"
            )
        for index, port in enumerate(self.ports):
            if self._reachable(port):
                raise RuntimeError(f"port {port} is already serving a server")
            log_path = os.path.join(self.root, f"node{index}.log")
            log = open(log_path, "wb")
            self.processes[index] = subprocess.Popen(
                self._node_args(index),
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        deadline = time.time() + timeout
        for index, port in enumerate(self.ports):
            while time.time() < deadline:
                if self.processes[index].poll() is not None:
                    raise RuntimeError(
                        f"node{index} exited early; see {self.root}/node{index}.log"
                    )
                if self._reachable(port):
                    break
                time.sleep(0.05)
            else:
                raise RuntimeError(f"node{index} did not start on port {port}")
        self._started = True
        config.PORTS = list(self.ports)
        self.ensure_db()
        return self

    def _reachable(self, port):
        try:
            requests.get(config.make_url(port), timeout=0.25)
            return True
        except requests.RequestException:
            return False

    def ensure_db(self):
        last = None
        for port in self.ports:
            try:
                last = requests.put(
                    config.make_url(port, config.DB), timeout=config.TIMEOUT
                )
                if last.status_code in (201, 202, 412):
                    break
            except requests.RequestException as error:
                last = error
        else:
            raise RuntimeError(f"could not create {config.DB}: {last}")

        deadline = time.time() + config.TIMEOUT
        while time.time() < deadline:
            if all(self.node_has_db(port) for port in self.ports):
                return
            time.sleep(0.1)
        missing = [port for port in self.ports if not self.node_has_db(port)]
        raise RuntimeError(f"database did not appear on nodes: {missing}")

    def node_has_db(self, port):
        try:
            response = requests.get(
                config.make_url(port, config.DB), timeout=2
            )
            return response.status_code == 200
        except requests.RequestException:
            return False

    def kill_node(self, index):
        process = self.processes.get(index)
        if process and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()

    def restart_node(self, index, timeout=30):
        self.kill_node(index)
        log_path = os.path.join(self.root, f"node{index}.log")
        log = open(log_path, "ab")
        self.processes[index] = subprocess.Popen(
            self._node_args(index),
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self._reachable(self.ports[index]):
                return
            if self.processes[index].poll() is not None:
                break
            time.sleep(0.05)
        raise RuntimeError(f"node{index} did not restart")

    def stop(self):
        for index in list(self.processes):
            self.kill_node(index)
        if self._started and not os.environ.get("DOUCHDB_KEEP_DATA"):
            shutil.rmtree(self.root, ignore_errors=True)

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc, tb):
        self.stop()
