"""Workloads, adapted from jepsen-couchdb to douchdb's HTTP API."""

import random as rand
import time

import requests

import config


def _node():
    return rand.choice(config.PORTS)


def _request(port, method, path, **kwargs):
    url = config.make_url(port, path)
    kwargs.setdefault("timeout", config.TIMEOUT)
    return requests.request(method, url, **kwargs)


def _create(port, value):
    response = _request(port, "POST", config.DB, json={"value": value})
    response.raise_for_status()
    result = response.json()
    result["_node"] = port
    result["_value"] = value
    return result


def _update(port, doc_id, rev, value):
    response = _request(
        port,
        "PUT",
        f"{config.DB}/{doc_id}",
        params={"rev": rev},
        json={"_id": doc_id, "_rev": rev, "value": value, "changed": True},
    )
    response.raise_for_status()
    result = response.json()
    result["_node"] = port
    result["_value"] = value
    return result


def _retry(operation, on_error=None):
    while True:
        try:
            return operation()
        except requests.RequestException:
            if on_error:
                on_error()
            time.sleep(0.05)


def random_work(n=100):
    results = []
    for i in range(n):
        results.append(_retry(lambda i=i: _create(_node(), i)))
    return results


def direct(n=100):
    index = 0
    results = []

    def next_node():
        nonlocal index
        node = config.PORTS[index % len(config.PORTS)]
        index += 1
        return node

    for i in range(n):
        results.append(
            _retry(lambda i=i: _create(next_node(), i), on_error=next_node)
        )
    return results


def sequential(n=100):
    index = 0
    results = []
    for i in range(n):
        last = None
        for _ in range(len(config.PORTS)):
            index += 1
            port = config.PORTS[index % len(config.PORTS)]
            try:
                results.append(_create(port, i))
                break
            except requests.RequestException as error:
                last = error
        else:
            raise last or RuntimeError("no reachable node")
    return results


def trainwreck(n=100):
    results = sequential(n)
    updated = []
    for i, result in enumerate(results):
        if "ok" not in result or "id" not in result or "rev" not in result:
            updated.append(result)
            continue
        index = i + 1
        port = config.PORTS[index % len(config.PORTS)]
        updated.append(
            _retry(
                lambda result=result, i=i, port=port: _update(
                    port, result["id"], result["rev"], i
                )
            )
        )
    return updated
