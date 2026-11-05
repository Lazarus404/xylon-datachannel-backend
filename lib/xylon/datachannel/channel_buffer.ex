defmodule Xylon.Datachannel.ChannelBuffer do
  @moduledoc """
  Bounded late-open queues for destinations missing a label channel.

  ## What problem this solves

  When `on_no_channel: :buffer`, a message for a joined peer that has not yet
  opened the label is queued briefly instead of skipped. Queues are keyed by
  `{plan_ref, dest_id, label}`, capped (`max_per_key` 8), TTL 5s, drop-oldest
  under pressure. Flush on channel open.

  ## Internal

  Used from `Xylon.Datachannel.Backend` when buffering is enabled.

  ## RFCs

  - Application policy only; transport is [RFC 8831](https://www.rfc-editor.org/rfc/rfc8831)
  """

  alias Xylon.Datachannel.Telemetry

  @table :xylon_datachannel_channel_buf
  @max_per_key 8
  @ttl_ms 5_000

  @doc "Ensures the channel-buffer ETS table exists."
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
  Enqueues `payload` for `{dest_id, label}`. Drops expired and overflow (oldest).
  """
  @spec push(term(), term(), binary(), binary(), map()) :: :ok
  def push(plan_ref, dest_id, label, payload, meta)
      when is_binary(label) and is_binary(payload) and is_map(meta) do
    ensure_table!()
    key = {plan_ref, dest_id, label}
    now = System.monotonic_time(:millisecond)
    entry = {now, payload}

    queue =
      case :ets.lookup(@table, key) do
        [{^key, q}] -> prune_expired(q, meta, now)
        [] -> []
      end

    queue = queue ++ [entry]

    queue =
      if length(queue) > @max_per_key do
        Telemetry.emit(:drop, %{count: 1}, Map.put(meta, :reason, :buffer_overflow))
        Enum.drop(queue, length(queue) - @max_per_key)
      else
        queue
      end

    true = :ets.insert(@table, {key, queue})
    :ok
  end

  @doc "Returns payloads (oldest first) without removing them."
  @spec peek(term(), term(), binary()) :: [binary()]
  def peek(plan_ref, dest_id, label) when is_binary(label) do
    ensure_table!()
    key = {plan_ref, dest_id, label}
    now = System.monotonic_time(:millisecond)

    case :ets.lookup(@table, key) do
      [{^key, q}] ->
        q
        |> prune_expired(%{}, now)
        |> Enum.map(fn {_t, p} -> p end)

      [] ->
        []
    end
  end

  @doc "Takes and clears the queue for `{dest_id, label}` (oldest first)."
  @spec take_all(term(), term(), binary()) :: [binary()]
  def take_all(plan_ref, dest_id, label) when is_binary(label) do
    ensure_table!()
    key = {plan_ref, dest_id, label}
    now = System.monotonic_time(:millisecond)

    case :ets.lookup(@table, key) do
      [{^key, q}] ->
        true = :ets.delete(@table, key)

        q
        |> prune_expired(%{}, now)
        |> Enum.map(fn {_t, p} -> p end)

      [] ->
        []
    end
  end

  @doc "Drops buffer rows for one participant."
  @spec clear_participant(term(), term()) :: :ok
  def clear_participant(plan_ref, participant_id) do
    ensure_table!()
    _ = :ets.match_delete(@table, {{plan_ref, participant_id, :_}, :_})
    :ok
  end

  @doc "Drops all buffer rows for a plan."
  @spec clear_plan(term()) :: :ok
  def clear_plan(plan_ref) do
    ensure_table!()
    _ = :ets.match_delete(@table, {{plan_ref, :_, :_}, :_})
    :ok
  end

  defp prune_expired(queue, meta, now) do
    {keep, dropped} =
      Enum.split_with(queue, fn {ts, _} -> now - ts <= @ttl_ms end)

    if dropped != [] do
      Telemetry.emit(:drop, %{count: length(dropped)}, Map.put(meta, :reason, :buffer_ttl))
    end

    keep
  end
end
