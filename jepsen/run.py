"""Start a disposable douchdb cluster and run one scenario."""

import argparse

import check
import cluster
import reset
import sync
import work


def main():
    parser = argparse.ArgumentParser(
        description="start a disposable douchdb cluster and run a jepsen scenario"
    )
    parser.add_argument(
        "check", choices=["simple", "sleep", "quorum", "trainwreck", "node_loss"]
    )
    parser.add_argument(
        "workload", choices=["random_work", "direct", "sequential", "trainwreck"]
    )
    parser.add_argument("--n", type=int, default=100)
    parser.add_argument("--nodes", type=int, default=3)
    args = parser.parse_args()

    with cluster.Cluster(nodes=args.nodes) as c:
        reset.reset(c.ports)
        sync.ensure(c)
        check_func = getattr(check, args.check)
        work_func = getattr(work, args.workload)
        if args.check == "node_loss":
            check_func(work_func, args.n, c)
        else:
            check_func(work_func, args.n)
        reset.reset(c.ports)


if __name__ == "__main__":
    main()
