# Xylon Datachannel Backend

Opaque WebRTC datachannel fanout and optional typed MFU for
[Xylon](https://github.com/Lazarus404/xylon). Transport (DTLS/SCTP) stays in
[XGW](https://github.com/Lazarus404/xgw); Session lifecycle stays in Xylon.

Ported and updated from an earlier Xirsys datachannel implementation; 
released for public use.

## What problem this solves

In a multiparty app, peers need a shared datachannel path for chat or
application messages without building a full mesh of PeerConnections. This
package is the `:datachannel` catalogue backend: attach it at boot, create
sessions with matching labels, and fan binaries to other participants (or
demux typed JSON or MessagePack via MFU).

## Main modules

- `Xylon.Datachannel` -- attach and server-side `publish/4`
- `Xylon.Datachannel.Backend` -- `Xylon.Backend` implementation
- `Xylon.Datachannel.Plan` -- ETS membership and label -> channel_ref maps
- `Xylon.Datachannel.Fanout` -- dest policy to forward specs
- `Xylon.Datachannel.Mfu` -- typed demux for `:mfu` mode
- `Xylon.Datachannel.Envelope` -- shared JSON shapes for chat / rpc / state
- `Xylon.Datachannel.RpcRouter` -- Internal MFU handlers for point-to-point RPC
- `Xylon.Datachannel.StateCoalesce` -- Internal stamp / stale-seq drop for `state`
- `Xylon.Datachannel.RateLimit` -- Internal per-sender token bucket
- `Xylon.Datachannel.ChannelBuffer` -- Internal late-open bounded queue
- `Xylon.Datachannel.Codec` / `Codec.JSON` / `Codec.Msgpack` / `Codec.Cbor` -- MFU codecs
- `Xylon.Datachannel.Telemetry` -- Internal `[:xylon, :dc, ...]` events
- `Xylon.Datachannel.Bench` -- Internal FakePeer fanout latency harness
- `Mix.Tasks.Xylon.Datachannel` -- HTTP Lab Mix task
- `Mix.Tasks.Xylon.Datachannel.Bench` -- N-peer forward latency CLI

## RFCs

- [RFC 8831](https://www.rfc-editor.org/rfc/rfc8831) (WebRTC data channels)
- [RFC 8832](https://www.rfc-editor.org/rfc/rfc8832) (data channel establishment)
- [RFC 8261](https://www.rfc-editor.org/rfc/rfc8261) (SCTP over DTLS; via XGW)
- [RFC 8829](https://www.rfc-editor.org/rfc/rfc8829) (JSEP; signaling through Xylon)
- [RFC 8445](https://www.rfc-editor.org/rfc/rfc8445) (ICE; via XGW / Xylon.Ice)
- [RFC 8259](https://www.rfc-editor.org/rfc/rfc8259) (JSON; default MFU codec)
- MessagePack (optional MFU codec via Msgpax)
- [RFC 8949](https://www.rfc-editor.org/rfc/rfc8949) (CBOR; optional MFU codec)

## Attach

```elixir
:ok = Xylon.Datachannel.attach()
# or: Xylon.attach(Xylon.Datachannel.Backend)

{:ok, session} =
  Xylon.Session.create(tenant_id, session_id, :datachannel,
    mode: :fanout_except_sender
  )
```

Catalogue id: `:datachannel`.

## Modes

| Mode | Behavior |
| --- | --- |
| `:fanout_all` | Deliver to all participants with a matching open label |
| `:fanout_except_sender` | Same, excluding the sender (default) |
| `:fanout_subset` | Deliver to an explicit participant-id list |
| `:mfu` | Decode -> type handlers / passthrough, then fanout |

### Game room (RPC + chat + state)

One Session mode `:mfu` with three labels. Enable the built-in RPC router so
`rpc.req` is delivered only to `to` (not the whole room):

```elixir
{:ok, session} =
  Xylon.Session.create(tenant_id, session_id, :datachannel,
    mode: :mfu,
    default_dest: :except_sender,
    labels: ["chat", "rpc", "state"],
    default_label: "chat",
    rpc_router: true,
    state_coalesce: true
  )
```

HTTP / WS `backend_opts` may set `"codec": "json"` (default), `"msgpack"`, or
`"cbor"`. Msgpack and CBOR Sessions set `dc_ppi: :binary` so Forwarder/Session
pass PPI 53 on both SendHandle passthrough MFA and `Peer.send_datachannel_async/4`.
Optional `label_codecs: %{"chat" => "json", "state" => "msgpack"}` overrides the
session codec per label (inbound decode + outbound encode/PPI). Clients should
open the `state` label with `ordered: false` and `maxRetransmits: 0` (see SDK
`STATE_CHANNEL_INIT`); server coalesce / client seq drop still apply under loss.

Opt-in server-open (after SCTP connect; default remains client-open):

```elixir
server_open_labels: ["rpc", "state"],
labels: ["chat", "rpc", "state"]
```

HTTP lab create body:

```json
{
  "session_id": "room-1",
  "library": "datachannel",
  "backend_opts": {
    "mode": "mfu",
    "default_dest": "except_sender",
    "labels": ["chat", "rpc", "state"],
    "default_label": "chat",
    "rpc_router": true,
    "state_coalesce": true
  }
}
```

Wire shapes: see `Xylon.Datachannel.Envelope`. Router: `Xylon.Datachannel.RpcRouter`.
State stamp / coalesce: `Xylon.Datachannel.StateCoalesce`.

Optional limits:

```elixir
rate_limit: {:token_bucket, 50, 100},
on_no_channel: :buffer   # default :skip
```

Inbound messages enter via `Xylon.Forwarder` (`{:forward_dc, ...}`). Membership
and label maps are ETS-backed. Channel close and SCTP disconnect clear label
maps without leaving the Session.

Cascade (trunk-like Participants on the same Plan): see
[ARCHITECTURE.md](ARCHITECTURE.md) section 13. Metrics panel sketch: section 12.

## Local deps

```bash
XTURN_LOCAL_DEPS=1 mix deps.get
XTURN_LOCAL_DEPS=1 mix test
```

## Lab boot

Build [xylon-test-client](https://github.com/Lazarus404/xylon-test-client)
(`npm run build`), then from this package:

```bash
XYLON_ADVERTISED_IP=127.0.0.1 XTURN_LOCAL_DEPS=1 mix xylon.datachannel --http-port 8080
```

`XYLON_ADVERTISED_IP` is required for host candidates (or set
`config :xylon, :ice`). Use `127.0.0.1` for local browsers; use a reachable
LAN/public IP otherwise. Optional Control WS full-ICE:
`XYLON_ICE_MODE=full` and `XYLON_STUN_SERVERS=...`. See
[xylon](https://github.com/Lazarus404/xylon) `Xylon.Ice`.

Open http://127.0.0.1:8080/client/ and use HTTP Datachannel Room
(two tabs, shared session id, different participant ids).

## Benchmark

In-node fanout cost (not browser RTT). FakePeers publish a DC SendHandle
passthrough (`send_path: dc_passthrough`). Production Forwarder prefers that
path when published, else `Peer.send_datachannel_async/4` (cast; SCTP+DTLS
still on the Peer GenServer -- not a full SCTP-in-ETS hot path). Session
`dc_ppi` (`:string` or `:binary` for MessagePack/CBOR) is passed on both the
SendHandle MFA and Peer async paths. Use `--send-path async_peer` and
`--publishers N` for Phase 12 Peer/Forwarder mailbox gates. Use
`--mode fanout_subset --subset-k K --label chat|state` for Phase 13 K-subset
density (`state` is label/path cost only; FakePeer does not simulate SCTP loss).

```bash
XTURN_LOCAL_DEPS=1 mix xylon.datachannel.bench --n 20 --pps 200 --duration 30 --hw "your-hw-class"
XTURN_LOCAL_DEPS=1 mix xylon.datachannel.bench --n 50 --pps 200 --duration 5 --publishers 5 --send-path async_peer --hw "your-hw-class" --json
XTURN_LOCAL_DEPS=1 mix xylon.datachannel.bench --n 50 --pps 200 --duration 5 --mode fanout_subset --subset-k 10 --label chat --hw "your-hw-class"
XTURN_LOCAL_DEPS=1 mix xylon.datachannel.bench --n 50 --pps 200 --duration 5 --mode fanout_subset --subset-k 10 --label state --hw "your-hw-class"
XTURN_LOCAL_DEPS=1 mix xylon.datachannel.bench --mode cascade_pipe --n 50 --pps 200 --duration 5 --hw "your-hw-class"
XTURN_LOCAL_DEPS=1 mix xylon.datachannel.bench --mode cascade_subset --n 50 --subset-k 10 --pps 200 --duration 5 --label chat --hw "your-hw-class"
```

Cascade modes use same-VM `Manager.connect_pair` + Pipe (FakePeer path; not
wire SCTP). Dual-BEAM OTP peer is not this harness. Aggregated numbers:
[xylon_bench/RESULTS.md](https://github.com/Lazarus404/xylon_bench/blob/main/RESULTS.md)
(DC Pipe cascade + Wire DC baseline sections). Same-host Chrome SCTP soak:
`./scripts/e2e-wire-baseline-dc.sh` from the workspace root (LAN
`XYLON_ADVERTISED_IP` required).

| Metric | Example (apple-local, N=20, 200 pps, 5s) |
| --- | --- |
| p50 forward | ~0.24 ms |
| p99 forward | ~1.13 ms |
| Session mailbox max | 0 |
| Forwarder mailbox max | 1 |
| send_path | `dc_passthrough` |

N=50 multi-pub gate (pps=200, ~3s): Peer mailbox max 1 on `async_peer`; Forwarder
mailbox max 1 on passthrough -- T12.4 / T12.5 WONTFIX (thresholds 50 / 100).

N=50 K=10 subset (pps=200, ~2s, passthrough): chat p50 ~0.09 ms / p99 ~0.22 ms;
state label path p50 ~0.11 ms / p99 ~0.41 ms; Forwarder mailbox max 1.

Same-VM Pipe cascade (N=50, 200 pps, 5s, `stamp_plane=payload`): see RESULTS
(`cascade_pipe` / `cascade_subset` K=10). Inject window uses Logger `:warning`
so debug route/fanout lines do not inflate p99.

Passthrough removes the Peer GenServer hop from the sample path; N=20 p99 is
still often dominated by Forwarder work and lab logging, so these numbers are a
reproduction baseline -- not a claim that DC density now matches RTP UDP
SendHandle. Pre-P4a GenServer.call baseline (same N/pps/duration): p50 ~0.28 ms,
p99 ~1.04 ms.

CI runs short ExUnit smokes (`bench_smoke_test.exs`,
`bench_cascade_smoke_test.exs`); density gates are manual.
Tune targets after longer runs on your hardware class.
