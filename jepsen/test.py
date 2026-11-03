"""Run a scenario against an already-running douchdb cluster."""

import argparse

import check
import config
import reset
import sync
import work


def main():
    parser = argparse.ArgumentParser(
        description="jepsen-couchdb scenarios adapted to douchdb"
    )
    parser.add_argument("check", choices=["simple", "sleep", "quorum", "trainwreck"])
    parser.add_argument(
        "workload", choices=["random_work", "direct", "sequential", "trainwreck"]
    )
    parser.add_argument("--n", type=int, default=100)
    parser.add_argument(
        "--no-reset", action="store_true", help="keep the existing test database"
    )
    args = parser.parse_args()

    if not args.no_reset:
        reset.reset()
    sync.ensure()
    check_func = getattr(check, args.check)
    work_func = getattr(work, args.workload)
    check_func(work_func, args.n)
    if not args.no_reset:
        reset.reset()


if __name__ == "__main__":
    main()
