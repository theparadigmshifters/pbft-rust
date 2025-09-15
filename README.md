# pBFT-rust

This repository contains a simple implementation of [Practical Byzantine Fault Tolerance Algorithm](https://pmg.csail.mit.edu/papers/osdi99.pdf) in Rust.

The project is an in-memory key-value store backed by pBFT.

## Running locally

Running multiple replicas locally can easily be done with [Overmind](https://github.com/DarthSim/overmind) (see [the Procfile](./kv-node/Procfile)). To do that use:
```
cd node
overmind start
```

This will start 4 replicas with built-in dev configuration. The default ports will be `[10000-10004]`.

You can also simply start replicas separately one by one in different sessions:
```
PBFT_DEV=0 ../target/release/node
PBFT_DEV=1 ../target/release/node
...
```
`PBFT_DEV` environment variable instructs the program to use built-in configuration for easy development and testing, the value denotes replica id and the correct values are [0-4].

Customization is possible with config files or environment variables.

### Logging

Use `RUST_LOG` environment variable to customize logging verbosity:
```bash
export RUST_LOG=kv_node=debug,pbft_core=debug
```

### Triggering View Change

In order to trigger the view change stop the leader replica (`replica0` when starting from scratch):
```bash
overmind stop replica0
```
> **NOTE**: Make sure the replica actually stops (the graceful shutdown may wait for open connections to close). If you are not sure, simply execute the stop command twice, which will issue a `SIGKILL`.

```
The request will fail, but the view change protocol should kick in in few seconds.

You can query the state endpoint to ensure the view has changed, and `replica1` is the leader now:
```bash
curl "localhost:10001/api/v1/pbft/state"
```
```json
{"replica_state":{"Leader":{"sequence":4}},"view":2,"last_applied":4,"last_stable_checkpoint_sequence":null}
```

> **NOTE**: There are likely quite a bit of bugs / not handled edge cases.
