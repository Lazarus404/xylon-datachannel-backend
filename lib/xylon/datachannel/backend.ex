defmodule Xylon.Datachannel.Backend do
  @moduledoc """
  `Xylon.Backend` implementation for opaque datachannel fanout and typed MFU.

  ## What problem this solves

  Xylon sessions need a catalogue module that reacts to join/leave, open/close
  datachannels, and inbound messages. This backend keeps membership and
  label -> channel_ref state in `Xylon.Datachannel.Plan`, then returns
  `{:forward_dc, specs, state}` so the Session (or Forwarder on the inbound
  hot path) can send via XGW Peers. Modes (`:fanout_except_sender`, `:mfu`,
  and others) choose who receives each payload; RTP/RTCP callbacks are no-ops
  because this library is datachannel-only. Inbound from `:trunk` role senders
  ignores embedded client `"to"` so cascade hops stay opaque on the receiving edge.

  Session and Forwarder each call `init/2` and hold a separate `backend_state`
  map. Both share one `plan_ref` (the Session pid when `:session_pid` is set).
  Plan ETS is the source of truth for membership and label maps; fanout reads
  `Plan.participants_map/1`, not a local participants mirror.

  Catalogue id: `:datachannel`.

  ## RFCs

  - [RFC 8831](https://www.rfc-editor.org/rfc/rfc8831) (WebRTC data channels)
  - [RFC 8832](https://www.rfc-editor.org/rfc/rfc8832) (data channel establishment)
  - [RFC 8261](https://www.rfc-editor.org/rfc/rfc8261) (SCTP over DTLS; transport in XGW)
  - [RFC 8829](https://www.rfc-editor.org/rfc/rfc8829) (JSEP session lifecycle in Xylon)
  """

  @behaviour Xylon.Backend

  require Logger

  alias Xylon.Datachannel.{
    ChannelBuffer,
    Envelope,
    EgressRoute,
    Fanout,
    Mfu,
    Plan,
    RateLimit,
    RpcRouter,
    StateCoalesce,
    Telemetry
  }

  alias Xylon.Datachannel.Codec.JSON

  @doc """
  Catalogue id for `Xylon.attach/1`.

  ## Examples

      iex> Xylon.Datachannel.Backend.id()
      :datachannel
  """
  @spec id() :: :datachannel
  def id, do: :datachannel

  @doc """
  Starts backend state for a Session.

  Builds a shared `Plan` (Session pid when `:session_pid` is set) and stores
  mode, labels, MFU codec/types, size limits, and optional authorize/tap hooks.

  When `rpc_router: true` (or a keyword list with `enabled: true`), merges
  `RpcRouter.types_map/2` into `:types` (router keys win for `rpc.*`). In `:mfu`
  mode, missing `"chat"` defaults to `:passthrough` and `"state"` uses a stamp
  (or coalesce) handler. When `state_coalesce: true`, stale `seq` ticks are dropped.
  Optional `rate_limit: {:token_bucket, max_per_sec, burst}` drops inbound floods
  and server `publish_datachannel/4` (bucket key `:publish`, override with
  `rate_limit_key:`). Optional `on_no_channel: :buffer` queues late-open dests.
  When `:codec` is `Xylon.Datachannel.Codec.Msgpack`, sets `dc_ppi: :binary` for
  Peer egress; otherwise `dc_ppi: :string`.
  """
  @impl true
  def init(session, opts) do
    plan_ref =
      case Keyword.get(opts, :session_pid) || Keyword.get(opts, :session) do
        pid when is_pid(pid) ->
          Plan.new(pid)

        other ->
          Logger.warning(
            "xylon datachannel Backend.init missing session_pid (got #{inspect(other)}); allocating private plan_ref"
          )

          Plan.new()
      end

    mode = Keyword.get(opts, :mode, :fanout_except_sender)
    types = build_types(plan_ref, mode, opts)
    rate_limit = RateLimit.normalize(Keyword.get(opts, :rate_limit))
    on_no_channel = normalize_on_no_channel(Keyword.get(opts, :on_no_channel, :skip))
    codec = Keyword.get(opts, :codec, JSON)
    dc_ppi = codec_ppi(codec)
    server_open_labels = normalize_server_open_labels(Keyword.get(opts, :server_open_labels, []))

    if rate_limit, do: RateLimit.ensure_table!()
    if on_no_channel == :buffer, do: ChannelBuffer.ensure_table!()

    state = %{
      plan_ref: plan_ref,
      session_id: session.id,
      tenant_id: session.tenant_id,
      mode: mode,
      default_dest: Keyword.get(opts, :default_dest, :except_sender),
      subset: Keyword.get(opts, :subset, []),
      labels: Keyword.get(opts, :labels, :any),
      codec: codec,
      dc_ppi: dc_ppi,
      label_codecs: normalize_label_codecs(Keyword.get(opts, :label_codecs, %{}), codec),
      types: types,
      unknown_type: Keyword.get(opts, :unknown_type, :drop),
      max_message_bytes: Keyword.get(opts, :max_message_bytes, 65_535),
      on_no_channel: on_no_channel,
      rate_limit: rate_limit,
      authorize: Keyword.get(opts, :authorize),
      tap: Keyword.get(opts, :tap),
      default_label: Keyword.get(opts, :default_label, "data"),
      rpc_router: Keyword.get(opts, :rpc_router, false),
      server_open_labels: server_open_labels
    }

    {:ok, state}
  end

  @doc """
  Builds the Forwarder plan row for this participant (datachannel mode).
  """
  @impl true
  @spec forward_plan(Xylon.Participant.t(), map()) :: {:ok, map(), map()}
  def forward_plan(participant, state) do
    {:ok, %{mode: :datachannel, participant: participant}, state}
  end

  @doc "Registers the participant in Plan ETS."
  @impl true
  def participant_joined(participant, state) do
    Plan.put_participant(state.plan_ref, participant)
    {:ok, state}
  end

  @doc "Removes the participant from Plan ETS and aux tables."
  @impl true
  def participant_left(participant, state) do
    RpcRouter.clear_participant(state.plan_ref, participant.id)
    StateCoalesce.clear_participant(state.plan_ref, participant.id)
    RateLimit.clear_participant(state.plan_ref, participant.id)
    ChannelBuffer.clear_participant(state.plan_ref, participant.id)
    EgressRoute.clear_participant(state.plan_ref, participant.id)
    Plan.delete_participant(state.plan_ref, participant.id)
    {:ok, state}
  end

  @doc "No-op; this backend does not process RTP."
  @impl true
  def handle_rtp(_participant, _rtp, state), do: {:ok, state}

  @doc "No-op; this backend does not process RTCP."
  @impl true
  def handle_rtcp(_participant, _rtcp, state), do: {:ok, state}

  @doc "Records an open datachannel label -> ref when the label is allowed; flushes buffer."
  @impl true
  def handle_datachannel_open(participant, %{ref: ref, label: label}, state) do
    if label_allowed?(state.labels, label) do
      Plan.put_participant(state.plan_ref, participant)
      Plan.put_channel(state.plan_ref, participant.id, label, ref)

      case flush_buffer(participant, label, ref, state) do
        [] ->
          {:ok, state}

        specs ->
          {:forward_dc, specs, state}
      end
    else
      {:ok, state}
    end
  end

  @doc "Drops one channel_ref from the participant's label map."
  @impl true
  def handle_datachannel_closed(participant, channel_ref, state) do
    Plan.delete_channel(state.plan_ref, participant.id, channel_ref)
    {:ok, state}
  end

  @doc "Clears all open channels for the participant (membership kept)."
  @impl true
  def handle_sctp_disconnected(participant, state) do
    Plan.clear_channels(state.plan_ref, participant.id)
    {:ok, state}
  end

  @doc """
  Opens opt-in `server_open_labels` on the participant Peer after SCTP connects.

  Skips labels already registered. Soft-fails open errors (returns `{:ok, state}`).
  """
  @spec maybe_server_open(Xylon.Participant.t(), map()) :: {:ok, map()}
  def maybe_server_open(participant, state) do
    Enum.reduce(state.server_open_labels, {:ok, state}, fn label, {:ok, st} ->
      open_server_label(participant, label, st)
    end)
  end

  defp open_server_label(participant, label, state) do
    cond do
      not label_allowed?(state.labels, label) ->
        {:ok, state}

      not is_nil(Plan.channel_ref(state.plan_ref, participant.id, label)) ->
        {:ok, state}

      not is_pid(participant.peer_ref) ->
        {:ok, state}

      true ->
        opts = server_open_opts(label)

        case XGW.Peer.open_datachannel(participant.peer_ref, label, opts) do
          {:ok, _info} ->
            {:ok, state}

          {:error, reason} ->
            Logger.debug(
              "xylon datachannel server_open fail label=#{inspect(label)} reason=#{inspect(reason)}"
            )

            {:ok, state}
        end
    end
  end

  defp server_open_opts(label) when label in ["state", "input"] do
    [ordered: false, reliability: :rexmit, param: 0]
  end

  defp server_open_opts(_label), do: [ordered: true, reliability: :reliable]

  @doc """
  Routes an inbound datachannel binary (size check, label lookup, fanout or MFU).

  Returns `{:forward_dc, specs, state}` or `{:ok, state}` when dropped.
  """
  @impl true
  def handle_datachannel(participant, channel_ref, data, state) when is_binary(data) do
    meta = base_meta(state)
    bytes = byte_size(data)

    if bytes > state.max_message_bytes do
      Logger.debug(
        "xylon datachannel drop reason=size from=#{inspect(participant.id)} bytes=#{bytes} max=#{state.max_message_bytes}"
      )

      Telemetry.emit(:drop, %{count: 1}, Map.put(meta, :reason, :size))
      maybe_tap(state, {:drop, :size, data})
      {:ok, state}
    else
      case RateLimit.take(state.plan_ref, participant.id, state.rate_limit) do
        :rate_limited ->
          Telemetry.emit(
            :drop,
            %{count: 1},
            meta
            |> Map.put(:reason, :rate_limit)
            |> Map.put(:sender_id, participant.id)
          )

          maybe_tap(state, {:drop, :rate_limit, data})
          {:ok, state}

        :ok ->
          label = Plan.label_for_ref(state.plan_ref, participant.id, channel_ref)

          label =
            if is_nil(label) do
              maybe_heal_unknown_channel(state, participant, channel_ref)
            else
              label
            end

          if is_nil(label) do
            members = Plan.participant_ids(state.plan_ref)
            channels = Plan.get_channels(state.plan_ref, participant.id)

            Logger.debug(
              "xylon datachannel drop reason=unknown_channel from=#{inspect(participant.id)} ref=#{inspect(channel_ref)} members=#{inspect(members)} channels=#{inspect(Map.keys(channels))}"
            )

            Telemetry.emit(:drop, %{count: 1}, Map.put(meta, :reason, :unknown_channel))
            {:ok, state}
          else
            Logger.debug(
              "xylon datachannel route from=#{inspect(participant.id)} label=#{inspect(label)} bytes=#{bytes} mode=#{inspect(state.mode)}"
            )

            meta =
              meta
              |> Map.put(:sender_id, participant.id)
              |> Map.put(:label, label)
              |> Map.put(:plan_ref, state.plan_ref)

            Telemetry.emit(:in, %{bytes: bytes}, meta)
            maybe_tap(state, {:in, participant.id, label, data})
            route_inbound(participant, label, data, state, meta)
          end
      end
    end
  end

  @doc """
  Server-originated publish: authorize, size-check, then fanout specs.

  Returns `{:forward_dc, specs, state}` or `{:error, reason}`.
  """
  @impl true
  def publish_datachannel(payload, dest, opts, state) when is_binary(payload) do
    meta = base_meta(state)
    label = Keyword.get(opts, :label, state.default_label)
    dest_policy = publish_dest(dest)
    dest_ids = dest_ids_for_auth(state.plan_ref, dest_policy, nil)
    sender_id = Keyword.get(opts, :participant_id) || Keyword.get(opts, :sender_id) || :publish

    auth_meta =
      %{
        dir: :publish,
        label: label,
        dest: dest_policy,
        dest_ids: dest_ids,
        sender_id: sender_id
      }
      |> put_capabilities_from_opts(opts)
      |> put_envelope_type(payload)

    if authorize?(state, auth_meta) != true do
      {:error, :unauthorized}
    else
      if byte_size(payload) > state.max_message_bytes do
        Telemetry.emit(:drop, %{count: 1}, Map.put(meta, :reason, :size))
        {:error, :too_large}
      else
        # Shared bucket for server inject (host / Control); optional override via opts.
        publish_key = Keyword.get(opts, :rate_limit_key, :publish)

        case RateLimit.take(state.plan_ref, publish_key, state.rate_limit) do
          :rate_limited ->
            Telemetry.emit(
              :drop,
              %{count: 1},
              meta
              |> Map.put(:reason, :rate_limit)
              |> Map.put(:sender_id, publish_key)
            )

            maybe_tap(state, {:drop, :rate_limit, payload})
            {:error, :rate_limited}

          :ok ->
            %{specs: specs, skipped: skipped, missing: missing} =
              Fanout.resolve(
                state.plan_ref,
                Plan.participants_map(state.plan_ref),
                dest_policy,
                nil,
                label,
                payload
              )

            buffered = maybe_buffer_missing(state, missing, meta)
            emit_fanout(meta, specs, skipped - buffered)
            maybe_tap(state, {:publish, dest, label, payload})
            {:forward_dc, specs, state}
        end
      end
    end
  end

  @doc "Deletes the Plan ETS rows for this Session."
  @impl true
  def terminate(_reason, state) do
    RpcRouter.clear_plan(state.plan_ref)
    StateCoalesce.clear_plan(state.plan_ref)
    RateLimit.clear_plan(state.plan_ref)
    ChannelBuffer.clear_plan(state.plan_ref)
    EgressRoute.clear_plan(state.plan_ref)
    Plan.delete(state.plan_ref)
    :ok
  end

  defp route_inbound(participant, label, data, %{mode: :mfu} = state, meta) do
    codec = codec_for(state, label)

    case Mfu.handle(data, codec, state.types, state.unknown_type, meta) do
      :drop ->
        {:ok, state}

      {:ok, :passthrough_unknown} ->
        fanout_payloads(participant, label, [{data, nil}], state, meta)

      {:ok, items} ->
        fanout_payloads(participant, label, items, state, meta)
    end
  end

  defp route_inbound(participant, label, data, state, meta) do
    fanout_payloads(participant, label, [{data, nil}], state, meta)
  end

  defp fanout_payloads(participant, label, items, state, meta) do
    state = put_label_ppi(state, label)
    default_dest = mode_dest(state)
    participants = Plan.participants_map(state.plan_ref)
    member_ids = MapSet.new(Map.keys(participants))

    {specs_rev, skipped, buffered, last_dest} =
      Enum.reduce(items, {[], 0, 0, default_dest}, fn {payload, dest_override},
                                                     {acc, skip, buf, last} ->
        case resolve_item_dest(
               payload,
               dest_override,
               default_dest,
               member_ids,
               meta,
               state,
               participant,
               label
             ) do
          :drop ->
            {acc, skip, buf, last}

          {:ok, dest} ->
            dest_ids = dest_ids_for_auth(state.plan_ref, dest, participant.id)

            auth_meta =
              %{
                dir: :send,
                label: label,
                dest: dest,
                dest_ids: dest_ids,
                sender_id: participant.id
              }
              |> put_capabilities(participant)
              |> put_envelope_type(payload)

            if authorize?(state, auth_meta) != true do
              Telemetry.emit(
                :drop,
                %{count: 1},
                meta
                |> Map.put(:reason, :unauthorized)
                |> Map.put(:sender_id, participant.id)
                |> Map.put(:label, label)
              )

              maybe_tap(state, {:drop, :unauthorized, payload})
              {acc, skip, buf, last}
            else
              %{specs: specs, skipped: s, missing: missing} =
                Fanout.resolve(
                  state.plan_ref,
                  participants,
                  dest,
                  participant.id,
                  label,
                  payload
                )

              buffered_n = maybe_buffer_missing(state, missing, meta)
              {Enum.reverse(specs, acc), skip + s, buf + buffered_n, dest}
            end
        end
      end)

    specs = Enum.reverse(specs_rev)
    dest_ids = Enum.map(specs, fn {p, _, _} -> p.id end)

    Logger.debug(
      "xylon datachannel fanout from=#{inspect(participant.id)} label=#{inspect(label)} dest=#{inspect(last_dest)} specs=#{length(specs)} skipped=#{skipped} dest_ids=#{inspect(dest_ids)} members=#{inspect(Plan.participant_ids(state.plan_ref))} plan=#{inspect(state.plan_ref)}"
    )

    emit_fanout(meta, specs, skipped - buffered)
    maybe_tap(state, {:out, length(specs), skipped})
    {:forward_dc, specs, state}
  end

  defp resolve_item_dest(_payload, dest_override, _default, member_ids, meta, _state, _sender, _label)
       when not is_nil(dest_override) do
    normalize_dest_policy(dest_override, member_ids, meta)
  end

  # Trunk hops stay opaque: ignore embedded client "to" so the receiving edge
  # uses Session default / egress (remote ids are not in the sending Plan).
  defp resolve_item_dest(payload, nil, default_dest, member_ids, meta, state, sender, label) do
    client =
      if sender.role == :trunk do
        :omit
      else
        client_dest_from_payload(payload, state, label)
      end

    case client do
      :omit ->
        case EgressRoute.get(state.plan_ref, sender.id, label) do
          {:ok, dest} ->
            normalize_dest_policy(dest, member_ids, meta, :empty_route)

          :default ->
            normalize_dest_policy(default_dest, member_ids, meta)
        end

      {:ok, ids} ->
        normalize_dest_policy(ids, member_ids, meta)

      :reject ->
        Telemetry.emit(:drop, %{count: 1}, Map.put(meta, :reason, :bad_dest))
        :drop
    end
  end

  defp normalize_dest_policy(dest, member_ids, meta),
    do: normalize_dest_policy(dest, member_ids, meta, :empty_dest)

  defp normalize_dest_policy(dest, member_ids, meta, empty_reason) when is_list(dest) do
    ids =
      dest
      |> Enum.filter(&MapSet.member?(member_ids, &1))
      |> Enum.uniq()

    if ids == [] do
      Telemetry.emit(:drop, %{count: 1}, Map.put(meta, :reason, empty_reason))
      :drop
    else
      {:ok, ids}
    end
  end

  defp normalize_dest_policy(dest, _member_ids, _meta, _empty_reason)
       when dest in [:all, :except_sender] do
    {:ok, dest}
  end

  defp normalize_dest_policy(_dest, _member_ids, meta, _empty_reason) do
    Telemetry.emit(:drop, %{count: 1}, Map.put(meta, :reason, :bad_dest))
    :drop
  end

  # Prefer label codec (or Session codec); Jason first is a fast path for UTF-8 JSON.
  defp client_dest_from_payload(payload, state, label) when is_binary(payload) do
    case decode_map_for_dest(payload, state, label) do
      {:ok, map} -> Envelope.dest_ids(map)
      :error -> :omit
    end
  end

  defp client_dest_from_payload(_, _, _), do: :omit

  defp decode_map_for_dest(payload, state, label) do
    codec = codec_for(state, label)

    case Jason.decode(payload) do
      {:ok, map} when is_map(map) ->
        {:ok, map}

      _ ->
        case codec.decode(payload) do
          {:ok, map} when is_map(map) -> {:ok, map}
          _ -> :error
        end
    end
  end

  defp codec_for(%{label_codecs: map, codec: default}, label)
       when is_map(map) and is_binary(label) do
    Map.get(map, label, default)
  end

  defp codec_for(%{codec: default}, _label), do: default

  defp put_label_ppi(state, label) do
    %{state | dc_ppi: codec_ppi(codec_for(state, label))}
  end

  defp dest_ids_for_auth(plan_ref, :all, _sender) do
    Plan.participant_ids(plan_ref)
  end

  defp dest_ids_for_auth(plan_ref, :except_sender, sender_id) do
    plan_ref
    |> Plan.participant_ids()
    |> Enum.reject(&(&1 == sender_id))
  end

  defp dest_ids_for_auth(_plan_ref, ids, _sender) when is_list(ids), do: ids

  defp maybe_buffer_missing(%{on_no_channel: :buffer} = state, missing, meta) do
    Enum.each(missing, fn {dest_id, label, payload} ->
      ChannelBuffer.push(state.plan_ref, dest_id, label, payload, meta)
    end)

    length(missing)
  end

  defp maybe_buffer_missing(_state, _missing, _meta), do: 0

  defp flush_buffer(participant, label, ref, %{on_no_channel: :buffer} = state) do
    payloads = ChannelBuffer.take_all(state.plan_ref, participant.id, label)
    Enum.map(payloads, fn payload -> {participant, ref, payload} end)
  end

  defp flush_buffer(_participant, _label, _ref, _state), do: []

  # When Session missed handle_datachannel_open (apply_offer vs DC-open race),
  # the first inbound still carries a live channel_ref. Register it under an
  # allowed label so fanout can proceed (Control lab chat is the common case).
  defp maybe_heal_unknown_channel(state, participant, channel_ref) do
    channels = Plan.get_channels(state.plan_ref, participant.id)

    if map_size(channels) != 0 do
      nil
    else
      heal_label =
        cond do
          label_allowed?(state.labels, "chat") -> "chat"
          label_allowed?(state.labels, state.default_label) -> state.default_label
          true -> nil
        end

      if is_binary(heal_label) do
        Logger.debug(
          "xylon datachannel heal unknown_channel id=#{inspect(participant.id)} label=#{inspect(heal_label)} ref=#{inspect(channel_ref)}"
        )

        Plan.put_participant(state.plan_ref, participant)
        Plan.put_channel(state.plan_ref, participant.id, heal_label, channel_ref)
        heal_label
      else
        nil
      end
    end
  end

  defp normalize_on_no_channel(:buffer), do: :buffer
  defp normalize_on_no_channel("buffer"), do: :buffer
  defp normalize_on_no_channel(_), do: :skip

  defp normalize_server_open_labels(list) when is_list(list) do
    list
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
    |> Enum.uniq()
  end

  defp normalize_server_open_labels(_), do: []

  defp codec_ppi(mod) when mod in [Xylon.Datachannel.Codec.Msgpack, Xylon.Datachannel.Codec.Cbor],
    do: :binary

  defp codec_ppi(_), do: :string

  defp normalize_label_codecs(map, default_codec) when is_map(map) do
    Map.new(map, fn
      {label, mod} when is_binary(label) and is_atom(mod) ->
        {label, resolve_codec(mod, default_codec)}

      {label, name} when is_binary(label) and is_binary(name) ->
        {label, resolve_codec_name(name, default_codec)}

      _ ->
        nil
    end)
    |> Enum.reject(&is_nil/1)
    |> Map.new()
  end

  defp normalize_label_codecs(_, _), do: %{}

  defp resolve_codec(mod, _default) when is_atom(mod) do
    case Code.ensure_loaded(mod) do
      {:module, ^mod} ->
        if function_exported?(mod, :decode, 1) do
          mod
        else
          raise ArgumentError, "bad codec #{inspect(mod)}"
        end

      _ ->
        raise ArgumentError, "bad codec #{inspect(mod)}"
    end
  end

  defp resolve_codec_name("json", _), do: JSON
  defp resolve_codec_name("msgpack", _), do: Xylon.Datachannel.Codec.Msgpack
  defp resolve_codec_name("cbor", _), do: Xylon.Datachannel.Codec.Cbor
  defp resolve_codec_name(other, _), do: raise(ArgumentError, "unknown codec #{inspect(other)}")

  defp mode_dest(%{mode: :fanout_all}), do: :all
  defp mode_dest(%{mode: :fanout_except_sender}), do: :except_sender
  defp mode_dest(%{mode: :fanout_subset, subset: subset}) when is_list(subset), do: subset
  defp mode_dest(%{mode: :mfu, default_dest: :all}), do: :all
  defp mode_dest(%{mode: :mfu, default_dest: :except_sender}), do: :except_sender
  defp mode_dest(%{mode: :mfu, default_dest: list}) when is_list(list), do: list
  defp mode_dest(_), do: :except_sender

  defp publish_dest(:all), do: :all
  defp publish_dest(ids) when is_list(ids), do: ids

  defp label_allowed?(:any, _label), do: true
  defp label_allowed?(labels, label) when is_list(labels), do: label in labels

  defp build_types(plan_ref, mode, opts) do
    user = normalize_types(Keyword.get(opts, :types, %{}))

    user =
      if mode == :mfu do
        Map.put_new(user, "chat", :passthrough)
      else
        user
      end

    user =
      if mode == :mfu do
        state_types =
          case state_coalesce_opts(Keyword.get(opts, :state_coalesce, false)) do
            nil -> StateCoalesce.stamp_types_map()
            coalesce_opts -> StateCoalesce.types_map(plan_ref, coalesce_opts)
          end

        # State handlers win over bare :passthrough; user explicit "state" wins over defaults.
        Map.merge(state_types, user)
      else
        user
      end

    case rpc_router_opts(Keyword.get(opts, :rpc_router, false)) do
      nil ->
        user

      router_opts ->
        Map.merge(user, RpcRouter.types_map(plan_ref, router_opts))
    end
  end

  defp state_coalesce_opts(true), do: []
  defp state_coalesce_opts(false), do: nil
  defp state_coalesce_opts(nil), do: nil

  defp state_coalesce_opts(opts) when is_list(opts) do
    if Keyword.get(opts, :enabled, true) == true, do: opts, else: nil
  end

  defp state_coalesce_opts(_), do: nil

  defp rpc_router_opts(true), do: []
  defp rpc_router_opts(false), do: nil
  defp rpc_router_opts(nil), do: nil

  defp rpc_router_opts(opts) when is_list(opts) do
    if Keyword.get(opts, :enabled, true) == true, do: opts, else: nil
  end

  defp rpc_router_opts(_), do: nil

  defp normalize_types(types) when is_map(types) do
    Map.new(types, fn
      {k, :passthrough} -> {normalize_type_key(k), :passthrough}
      {k, fun} when is_function(fun) -> {normalize_type_key(k), fun}
    end)
  end

  defp normalize_type_key(k) when is_atom(k), do: Atom.to_string(k)
  defp normalize_type_key(k) when is_binary(k), do: k

  defp authorize?(%{authorize: nil}, _meta), do: true

  defp authorize?(%{authorize: fun}, meta) when is_function(fun, 1) do
    fun.(meta) == true
  end

  # Defensive: Control conference authorize is (action, meta). Map :dir to action.
  defp authorize?(%{authorize: fun}, meta) when is_function(fun, 2) do
    action = Map.get(meta, :dir, :send)
    fun.(action, meta) == true
  end

  defp put_capabilities(meta, %Xylon.Participant{meta: pmeta}) when is_map(pmeta) do
    case Map.fetch(pmeta, :capabilities) do
      {:ok, caps} -> Map.put(meta, :capabilities, caps)
      :error -> meta
    end
  end

  defp put_capabilities(meta, _), do: meta

  defp put_capabilities_from_opts(meta, opts) when is_list(opts) do
    case Keyword.fetch(opts, :capabilities) do
      {:ok, caps} -> Map.put(meta, :capabilities, caps)
      :error -> meta
    end
  end

  # Cheap JSON `"type"` peek; skip non-JSON / msgpack.
  defp put_envelope_type(meta, <<"{\"type\":\"", rest::binary>>) do
    case :binary.split(rest, "\"") do
      [type, _] when type != "" -> Map.put(meta, :type, type)
      _ -> meta
    end
  end

  defp put_envelope_type(meta, _), do: meta

  defp maybe_tap(%{tap: nil}, _event), do: :ok

  defp maybe_tap(%{tap: fun}, event) when is_function(fun, 1) do
    try do
      fun.(event)
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end

    :ok
  end

  defp emit_fanout(meta, specs, skipped) do
    Telemetry.emit(:out, %{count: length(specs)}, meta)
    Telemetry.emit(:fanout_size, %{count: length(specs)}, meta)

    if skipped > 0 do
      Telemetry.emit(:skip_no_channel, %{count: skipped}, meta)
    end

    :ok
  end

  defp base_meta(state) do
    %{
      tenant_id: state.tenant_id,
      session_id: state.session_id,
      mode: state.mode
    }
  end
end
