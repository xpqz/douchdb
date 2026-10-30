# Development process

1. **Track every task as a skis issue.** Tasks are linked to the epic; bugs and
   spikes use the same tracker.
2. **Branch from `main` as `{issue-id}-{slug}`**, for example
   `21-opentelemetry`.
3. Implement the issue, add tests, and run `cargo test` and
   `cargo clippy --all-targets`.
4. Commit on the branch and merge with `--no-ff` so the merge commit is a
   rollback point:
   ```sh
   git switch main
   git merge --no-ff {branch} -m "Merge issue #N: ..."
   git branch -d {branch}
   ```
5. Close the skis issue with the merge commit and test evidence in the comment.
6. Push `main`. If a merge is wrong, `git revert -m 1 <merge-commit>` restores
   the previous state; individual commits remain available on the merge.
7. Never leave an issue branch open after its merge, and never commit unrelated
   work to a task branch.
