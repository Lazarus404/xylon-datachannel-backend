defmodule Xylon.Datachannel do
  @moduledoc """
  Datachannel fanout and typed MFU media library for Xylon.

  ## What problem this solves

  WebRTC audio/video is not the only realtime path: browsers also open SCTP
  datachannels for chat, game state, or application messages. In a room, each
  peer's messages usually need to reach other peers that have the same label
  open. Doing that as a full mesh of PeerConnections wastes setup; doing it on
  the media server means one join, then the server fans messages out.

  This package is the `:datachannel` catalogue backend for
  [Xylon](https://github.com/Lazarus404/xylon). Call `attach/0` at boot (or list
  `Xylon.Datachannel.Backend` in `config :xylon, :backends`), then create
  sessions with catalogue id `:datachannel`. Opaque modes copy binaries by
  label; `:mfu` decodes typed messages, runs handlers, then fans out. Transport
  (DTLS/SCTP) stays in [XGW](https://github.com/Lazarus404/xgw); Session I/O
  stays in Xylon.

  ## Main modules

  - `Xylon.Datachannel` -- attach, `publish/4`, and `set_route` / `clear_route`
  - `Xylon.Datachannel.Backend` -- `Xylon.Backend` implementation
  - `Xylon.Datachannel.Plan` -- ETS membership and label -> channel_ref maps
  - `Xylon.Datachannel.Fanout` -- dest policy to forward specs
  - `Xylon.Datachannel.Mfu` -- typed demux for `:mfu` mode
  - `Xylon.Datachannel.Codec` -- codec behaviour for MFU
  - `Xylon.Datachannel.Codec.JSON` -- default JSON codec (`"type"` field)
  - `Xylon.Datachannel.Codec.Msgpack` -- MessagePack codec (`"type"` field)
  - `Xylon.Datachannel.Envelope` -- shared JSON wire shapes (chat / rpc / state)
  - `Xylon.Datachannel.EgressRoute` -- Internal per-sender default dest map
  - `Xylon.Datachannel.RpcRouter` -- Internal point-to-point RPC MFU handlers
  - `Xylon.Datachannel.StateCoalesce` -- Internal `state` stamp / stale-seq drop
  - `Xylon.Datachannel.RateLimit` -- Internal per-sender token bucket
  - `Xylon.Datachannel.ChannelBuffer` -- Internal late-open bounded queue
  - `Xylon.Datachannel.Telemetry` -- Internal `[:xylon, :dc, ...]` events
  - `Xylon.Datachannel.Bench` -- Internal FakePeer fanout latency harness

  ## RFCs

  - [RFC 8831](https://www.rfc-editor.org/rfc/rfc8831) (WebRTC data channels)
  - [RFC 8832](https://www.rfc-editor.org/rfc/rfc8832) (data channel establishment)
  - [RFC 8261](https://www.rfc-editor.org/rfc/rfc8261) (SCTP over DTLS; via XGW)
  - [RFC 8829](https://www.rfc-editor.org/rfc/rfc8829) (JSEP; signaling through Xylon)
  - [RFC 8445](https://www.rfc-editor.org/rfc/rfc8445) (ICE; via XGW / Xylon.Ice)
  """

  alias Xylon.Datachannel.Backend

  @doc """
  Catalogue id for `Xylon.attach/1` and `Xylon.Session.create/3`.

  ## Examples

      iex> Xylon.Datachannel.id()
      :datachannel
  """
  @spec id() :: :datachannel
  def id, do: :datachannel

  @doc """
  Registers `Xylon.Datachannel.Backend` with the Xylon catalogue.

  ## Examples

      iex> Xylon.Datachannel.attach()
      :ok
  """
  @spec attach() :: :ok | {:error, :invalid_backend}
  def attach do
    :ok = Xylon.Datachannel.Plan.init()
    Xylon.attach(Backend)
  end

  @doc """
  Server-originated datachannel send into an existing Session.

  `dest` is `:all` or a list of participant ids. Optional `:label` selects the
  datachannel label (default from Session backend opts).
  """
  @spec publish(pid(), binary(), :all | [term()], keyword()) :: :ok | {:error, term()}
  def publish(session, payload, dest, opts \\ [])
      when is_pid(session) and is_binary(payload) and is_list(opts) do
    Xylon.Session.publish_datachannel(session, payload, dest, opts)
  end

  @doc """
  Sets a per-sender default dest for inbound fanout (no renegotiation).

  `dest` is `:all`, `:except_sender`, or a participant id list (empty list means
  drop until cleared). Optional `:label` scopes the route; omit for label-agnostic
  (`:_`). Precedence: MFU override > client `"to"` > this route > Session default.
  `sender_id` must already be in the Session Plan.
  """
  @spec set_route(pid(), term(), :all | :except_sender | [term()], keyword()) ::
          :ok | {:error, term()}
  def set_route(session, sender_id, dest, opts \\ [])
      when is_pid(session) and is_list(opts) do
    case Xylon.Session.plan_ref(session) do
      nil ->
        {:error, :not_supported}

      plan_ref ->
        if sender_id in Xylon.Datachannel.Plan.participant_ids(plan_ref) do
          label = Keyword.get(opts, :label, :_)

          case Xylon.Datachannel.EgressRoute.put(plan_ref, sender_id, dest, label) do
            :ok ->
              Xylon.Datachannel.Telemetry.emit(
                :route,
                %{count: 1},
                %{action: :set, sender_id: sender_id, label: label}
              )

              :ok

            {:error, _} = err ->
              err
          end
        else
          {:error, :unknown_sender}
        end
    end
  end

  @doc """
  Clears a per-sender egress route so inbound fanout uses Session default again.

  With `:label`, clears that one row. Without `:label`, clears **all** labels for
  `sender_id`.
  """
  @spec clear_route(pid(), term(), keyword()) :: :ok | {:error, term()}
  def clear_route(session, sender_id, opts \\ [])
      when is_pid(session) and is_list(opts) do
    case Xylon.Session.plan_ref(session) do
      nil ->
        {:error, :not_supported}

      plan_ref ->
        case Keyword.fetch(opts, :label) do
          {:ok, label} ->
            :ok = Xylon.Datachannel.EgressRoute.clear(plan_ref, sender_id, label)

            Xylon.Datachannel.Telemetry.emit(
              :route,
              %{count: 1},
              %{action: :clear, sender_id: sender_id, label: label}
            )

          :error ->
            :ok = Xylon.Datachannel.EgressRoute.clear_sender(plan_ref, sender_id)

            Xylon.Datachannel.Telemetry.emit(
              :route,
              %{count: 1},
              %{action: :clear, sender_id: sender_id, label: :all}
            )
        end

        :ok
    end
  end

  @doc "Lists egress routes for a Session (`plan_ref`)."
  @spec list_routes(pid()) :: [map()] | {:error, term()}
  def list_routes(session) when is_pid(session) do
    case Xylon.Session.plan_ref(session) do
      nil -> {:error, :not_supported}
      plan_ref -> Xylon.Datachannel.EgressRoute.list(plan_ref)
    end
  end
end
