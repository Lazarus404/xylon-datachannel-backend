defmodule Xylon.Datachannel.StateCoalesce do
  @moduledoc """
  MFU handlers for Unity-style `state` ticks: stamp `from` and optionally drop stale seq.

  ## What problem this solves

  High-rate state updates should not queue forever. This module stamps the sender
  id onto each envelope so receivers can key last-seq per peer, and when
  `state_coalesce` is enabled drops inbound ticks with `seq <=` the last seen
  value for `{sender, topic}` before fanout (saves egress).

  ## Internal

  Used from `Xylon.Datachannel.Backend` in `:mfu` mode. Pending/last-seq lives in
  ETS keyed by `plan_ref` (shared across Session and Forwarder).

  ## RFCs

  - Application framing only; transport is [RFC 8831](https://www.rfc-editor.org/rfc/rfc8831)
  """

  alias Xylon.Datachannel.{Envelope, Telemetry}

  @table :xylon_datachannel_state_seq
  @default_label "state"

  @doc "Ensures the state-seq ETS table exists."
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
  MFU type map with stamp-only handler (no server-side seq drop).
  """
  @spec stamp_types_map() :: %{optional(binary()) => function()}
  def stamp_types_map do
    %{"state" => fn decoded, meta -> handle_stamp(decoded, meta) end}
  end

  @doc """
  MFU type map with coalesce (drop stale) + stamp, closed over `plan_ref`.
  """
  @spec types_map(term(), keyword()) :: %{optional(binary()) => function()}
  def types_map(plan_ref, opts \\ []) do
    ensure_table!()
    strict = Keyword.get(opts, :label, @default_label)

    %{
      "state" => fn decoded, meta -> handle_coalesce(decoded, meta, plan_ref, strict) end
    }
  end

  @doc "Drops seq rows for one sender (on leave)."
  @spec clear_participant(term(), term()) :: :ok
  def clear_participant(plan_ref, participant_id) do
    ensure_table!()
    _ = :ets.match_delete(@table, {{plan_ref, participant_id, :_}, :_})
    :ok
  end

  @doc "Drops all seq rows for a plan (Session terminate)."
  @spec clear_plan(term()) :: :ok
  def clear_plan(plan_ref) do
    ensure_table!()
    _ = :ets.match_delete(@table, {{plan_ref, :_, :_}, :_})
    :ok
  end

  defp handle_stamp(decoded, meta) do
    case Envelope.validate(decoded) do
      {:ok, %{type: "state"}} ->
        sender = Map.get(meta, :sender_id)

        if is_binary(sender) and sender != "" do
          {:forward, Map.put(decoded, "from", sender)}
        else
          drop(meta, :state_missing_sender)
        end

      _ ->
        drop(meta, :state_invalid)
    end
  end

  defp handle_coalesce(decoded, meta, plan_ref, strict) do
    if label_ok?(meta, strict) do
      case Envelope.validate(decoded) do
        {:ok, %{type: "state", topic: topic, seq: seq}} ->
          sender = Map.get(meta, :sender_id)

          if is_binary(sender) and sender != "" do
            case put_if_newer(plan_ref, sender, topic, seq) do
              :stale ->
                drop(meta, :stale_state)

              :ok ->
                {:forward, Map.put(decoded, "from", sender)}
            end
          else
            drop(meta, :state_missing_sender)
          end

        _ ->
          drop(meta, :state_invalid)
      end
    else
      drop(meta, :state_wrong_label)
    end
  end

  defp put_if_newer(plan_ref, sender, topic, seq) do
    ensure_table!()
    key = {plan_ref, sender, topic}

    case :ets.lookup(@table, key) do
      [{^key, last}] when seq <= last ->
        :stale

      _ ->
        true = :ets.insert(@table, {key, seq})
        :ok
    end
  end

  defp label_ok?(_meta, nil), do: true
  defp label_ok?(_meta, ""), do: true

  defp label_ok?(meta, strict) when is_binary(strict) do
    Map.get(meta, :label) == strict
  end

  defp drop(meta, reason) do
    Telemetry.emit(:drop, %{count: 1}, Map.put(meta, :reason, reason))
    :drop
  end
end
