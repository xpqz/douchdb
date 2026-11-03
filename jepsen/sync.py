"""DouchDB has built-in anti-entropy, so there is no _replicator setup."""

import time

import config


def ensure(cluster=None):
    if cluster is not None:
        cluster.ensure_db()
    # Give anti-entropy a moment to bring every replica up to date.
    time.sleep(float(config.SYNC_SECS) * 2)


def partition(*_args, **_kwargs):
    raise NotImplementedError(
        "douchdb has no per-peer replication switch. Use a node kill/restart "
        "or control the container network (docker network disconnect / "
        "iptables) to simulate a partition; see jepsen/README.md."
    )
