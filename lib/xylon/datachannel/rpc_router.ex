defmodule Xylon.Datachannel.RpcRouter do
  @moduledoc """
  MFU handlers that route peer RPC to unicast, multicast, or broadcast destinations.

  ## What problem this solves

  Opaque fanout would deliver `rpc.req` to every peer. This router stamps
  `from`, fans out to `"to"` (participant id, id list, or `"*"` for
  `:except_sender`), and returns `rpc.res` / `rpc.err` to the original caller
  via an ETS pending table keyed by `plan_ref` (shared across Session and
  Forwarder backend state copies). One pending row per request `id`; the first
  reply wins (broadcast/multicast is notify/fan-out, not collect-N).

  `"*"` is RpcRouter-only; chat / media-path `Envelope.dest_ids/1` does not
  treat `"*"` as room broadcast.

  ## Internal

  Used when Session opts set `rpc_router: true` (or a keyword list). Not a
  public game-server API.

  ## RFCs

  - Application framing only; transport is [RFC 8831](https://www.rfc-editor.org/rfc/rfc8831)
  """

  alias Xylon.Datachannel.{Envelope, Telemetry}

  @table :xylon_datachannel_rpc_pending
  @default_ttl_ms 60_000

  @doc "Ensures the pending ETS table exists."
  @spec ensure_table!() :: :ok
  def ensure_table! do
    case :ets.whereis(@table) do
      :undefined ->
        _ =
          :ets.new(@table, [
            :named_table,
            :public,
            :set,
            read_concurrency: true,
            write_concurrency: true
          ])

        :ok

      _tid ->
        :ok
    end
  end

  @doc """
  MFU type map closed over `plan_ref` and router opts.

  Router keys win for `rpc.req` / `rpc.res` / `rpc.err`.
  """
  @spec types_map(term(), keyword()) :: %{optional(binary()) => function()}
  def types_map(plan_ref, opts \\ []) do
    ensure_table!()
    ttl = Keyword.get(opts, :pending_ttl_ms, @default_ttl_ms)
    strict = Keyword.get(opts, :strict_label, "rpc")

    %{
      "rpc.req" => fn decoded, meta -> handle_req(decoded, meta, plan_ref, ttl, strict) end,
      "rpc.res" => fn decoded, meta -> handle_res(decoded, meta, plan_ref, strict) end,
      "rpc.err" => fn decoded, meta -> handle_err(decoded, meta, plan_ref, strict) end
    }
  end

  @doc "Drops pending rows for one caller (on leave)."
  @spec clear_participant(term(), term()) :: :ok
  def clear_participant(plan_ref, participant_id) do
    ensure_table!()

    match = {{plan_ref, :"$1"}, participant_id, :_}
    _ = :ets.match_delete(@table, match)
    :ok
  end

  @doc "Drops all pending rows for a plan (Session terminate)."
  @spec clear_plan(term()) :: :ok
  def clear_plan(plan_ref) do
    ensure_table!()
    _ = :ets.match_delete(@table, {{plan_ref, :_}, :_, :_})
    :ok
  end

  defp handle_req(decoded, meta, plan_ref, ttl, strict) do
    if label_ok?(meta, strict) do
      case Envelope.validate(decoded) do
        {:ok, %{type: "rpc.req", id: id, to: to}} ->
          sender = Map.get(meta, :sender_id)

          if is_binary(sender) and sender != "" do
            put_pending(plan_ref, id, sender, ttl)
            out = Map.put(decoded, "from", sender)
            {:forward, out, fanout_dest(to)}
          else
            drop(meta, :rpc_missing_sender)
          end

        _ ->
          drop(meta, :rpc_invalid)
      end
    else
      drop(meta, :rpc_wrong_label)
    end
  end

  defp fanout_dest({:unicast, id}), do: [id]
  defp fanout_dest(:except_sender), do: :except_sender
  defp fanout_dest({:ids, ids}), do: ids

  defp handle_res(decoded, meta, plan_ref, strict) do
    route_reply(decoded, meta, plan_ref, strict, "rpc.res")
  end

  defp handle_err(decoded, meta, plan_ref, strict) do
    route_reply(decoded, meta, plan_ref, strict, "rpc.err")
  end

  defp route_reply(decoded, meta, plan_ref, strict, expect_type) do
    if label_ok?(meta, strict) do
      case Envelope.validate(decoded) do
        {:ok, %{type: ^expect_type, id: id} = env} ->
          dest =
            case env do
              %{to: to} when is_binary(to) and to != "" ->
                _ = take_pending(plan_ref, id)
                to

              _ ->
                take_pending(plan_ref, id)
            end

          if is_binary(dest) and dest != "" do
            {:forward, decoded, [dest]}
          else
            drop(meta, :rpc_unknown_pending)
          end

        _ ->
          drop(meta, :rpc_invalid)
      end
    else
      drop(meta, :rpc_wrong_label)
    end
  end

  defp label_ok?(_meta, nil), do: true
  defp label_ok?(_meta, ""), do: true

  defp label_ok?(meta, strict) when is_binary(strict) do
    Map.get(meta, :label) == strict
  end

  defp put_pending(plan_ref, id, caller_id, ttl_ms) do
    ensure_table!()
    expires = System.monotonic_time(:millisecond) + ttl_ms
    true = :ets.insert(@table, {{plan_ref, id}, caller_id, expires})
    :ok
  end

  defp take_pending(plan_ref, id) do
    ensure_table!()
    now = System.monotonic_time(:millisecond)

    case :ets.take(@table, {plan_ref, id}) do
      [{_, caller_id, expires}] when expires >= now -> caller_id
      [{_, _caller_id, _expires}] -> nil
      [] -> nil
    end
  end

  defp drop(meta, reason) do
    Telemetry.emit(:drop, %{count: 1}, Map.put(meta, :reason, reason))
    :drop
  end
end
