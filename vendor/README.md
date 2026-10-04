# Vendored Crates

## iroh

`vendor/iroh` is iroh 1.0.3 from crates.io with one change. The change is in
`src/socket/remote_map/remote_state.rs`. The function `queue_open_path` does
not add an address that is in the retry queue.

Without the change, a path that cannot open goes back into the retry queue.
It goes back one time for each connection to the peer. The retry runs each 333
milliseconds. With two or more connections, the queue grows exponentially, and
the memory of the listener doubles approximately each 333 milliseconds. The
upstream issue is <https://github.com/n0-computer/iroh/issues/4390>.

To show the change, compare the copy with the crates.io source:

```bash
diff -r ~/.cargo/registry/src/index.crates.io-*/iroh-1.0.3/src vendor/iroh/src
```

To do a test of the change:

```bash
cargo test --manifest-path vendor/iroh/Cargo.toml --locked --lib queue_open_path
```

When an iroh release fixes the issue, remove the copy in one change:

1. Remove `vendor/iroh`.
2. Remove `exclude` and the `[patch.crates-io]` entry in `Cargo.toml`.
3. Remove the `COPY vendor` line in `Dockerfile`.
4. Remove the step `Test the iroh patch` in `.github/workflows/ci.yml`.
5. Update iroh to the fixed release.
