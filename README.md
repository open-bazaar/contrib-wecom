# bazaar-contrib-wecom

WeCom Channel provider for [Bazaar Compute Node](https://github.com/yuchanns/bazaar-compute-node).

## Setup

Create a WeCom intelligent robot in API mode with a long connection, then install BCN and this provider:

```sh
uv tool install bazaar-compute-node
bcn contrib add wecom --enable
```

Add an agent using an installed Harness provider:

```sh
bcn agent add --name Editor --channel wecom --harness HARNESS_KIND
bcn run
```

## Configuration

Set these fields in the agent's `wecom` Channel configuration:

- `bot_id`: The robot's bot ID.
- `secret_env`: Environment variable holding the robot's secret.
- `secret`: Secret supplied directly by the configer; takes precedence.
- `websocket_url`: Long connection endpoint; defaults to `wss://openws.work.weixin.qq.com`.
