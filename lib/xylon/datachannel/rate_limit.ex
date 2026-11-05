defmodule Xylon.Datachannel.RateLimit do
  @moduledoc """
  Per-sender token-bucket rate limit for inbound datachannel messages.

  ## What problem this solves

  Dense rooms need a cheap drop path when one peer floods chat or state ticks.
  This module keeps `{tokens, last_ms}` in ETS keyed by `{plan_ref, sender_id}`
  so Session and Forwarder share one bucket per participant.

  ## Internal

  Used from `Xylon.Datachannel.Backend` when `rate_limit: {:token_bucket, max_per_sec, burst}`.

  ## RFCs

  - Application policy only; no wire RFC
  """

  @table :xylon_datachannel_rate

  @doc "Ensures the rate-limit ETS table exists."
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
  Consumes one token for `sender_id` under `cfg`.

  `cfg` is `nil` (always allow) or `{:token_bucket, max_per_sec, burst}`.
  """
  @spec take(term(), term(), nil | {:token_bucket, pos_integer(), pos_integer()}) ::
          :ok | :rate_limited
  def take(_plan_ref, _sender_id, nil), do: :ok

  def take(plan_ref, sender_id, {:token_bucket, max_per_sec, burst})
      when is_integer(max_per_sec) and max_per_sec > 0 and is_integer(burst) and burst > 0 do
    ensure_table!()
    key = {plan_ref, sender_id}
    now = System.monotonic_time(:millisecond)

    case :ets.lookup(@table, key) do
      [{^key, tokens, last_ms}] ->
        tokens = refill(tokens, last_ms, now, max_per_sec, burst)

        if tokens >= 1.0 do
          true = :ets.insert(@table, {key, tokens - 1.0, now})
          :ok
        else
          true = :ets.insert(@table, {key, tokens, now})
          :rate_limited
        end

      [] ->
        # First message: start at burst-1 remaining after this take.
        true = :ets.insert(@table, {key, burst * 1.0 - 1.0, now})
        :ok
    end
  end

  @doc "Drops rate rows for one sender (on leave)."
  @spec clear_participant(term(), term()) :: :ok
  def clear_participant(plan_ref, participant_id) do
    ensure_table!()
    _ = :ets.delete(@table, {plan_ref, participant_id})
    :ok
  end

  @doc "Drops all rate rows for a plan (Session terminate)."
  @spec clear_plan(term()) :: :ok
  def clear_plan(plan_ref) do
    ensure_table!()
    _ = :ets.match_delete(@table, {{plan_ref, :_}, :_, :_})
    :ok
  end

  @doc "Normalizes Session keyword opt into internal cfg or nil."
  @spec normalize(term()) :: nil | {:token_bucket, pos_integer(), pos_integer()}
  def normalize(nil), do: nil
  def normalize(false), do: nil
  def normalize({:token_bucket, max, burst}) when is_integer(max) and is_integer(burst), do: {:token_bucket, max, burst}

  def normalize(%{max_per_sec: max, burst: burst}) when is_integer(max) and is_integer(burst) do
    {:token_bucket, max, burst}
  end

  def normalize(%{"max_per_sec" => max, "burst" => burst})
      when is_integer(max) and is_integer(burst) do
    {:token_bucket, max, burst}
  end

  def normalize(_), do: nil

  defp refill(tokens, last_ms, now, max_per_sec, burst) do
    elapsed = max(0, now - last_ms)
    tokens = tokens + elapsed / 1000.0 * max_per_sec
    min(tokens, burst * 1.0)
  end
end
