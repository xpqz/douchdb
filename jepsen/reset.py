"""Delete the test database and wait for all configured nodes to agree."""

import time

import requests

import config


def reset(ports=None, timeout=None):
    ports = ports or config.PORTS
    timeout = timeout or config.TIMEOUT
    for port in ports:
        try:
            requests.delete(
                config.make_url(port, config.DB), timeout=config.TIMEOUT
            )
            break
        except requests.RequestException:
            continue

    deadline = time.time() + timeout
    while time.time() < deadline:
        gone = True
        for port in ports:
            try:
                response = requests.get(
                    config.make_url(port, config.DB), timeout=2
                )
            except requests.RequestException:
                continue
            if response.status_code != 404:
                gone = False
                break
        if gone:
            print("Reset.")
            return
        time.sleep(0.1)
    raise RuntimeError("database did not disappear from every node")
