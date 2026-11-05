defmodule Xylon.Datachannel.Telemetry do
  @moduledoc """
  Internal telemetry helper for datachannel Backend and MFU.

  ## What problem this solves

  Operators and tests need counters for inbound bytes, fanout size, and drops
  (size, unknown channel, decode, unknown type). This module wraps
  `:telemetry.execute/3` under `[:xylon, :dc, event]` so call sites stay short.

  ## Internal

  Not part of the public attach/publish API. Subscribe with `:telemetry.attach/4`
  if you need metrics.

  ## RFCs

  - Observability only; no wire RFC
  """

  @doc """
  Emits `[:xylon, :dc, event]` with `measurements` and `meta`.
  """
  @spec emit(atom(), map(), map()) :: :ok
  def emit(event, measurements \\ %{}, meta \\ %{})
      when is_atom(event) and is_map(measurements) and is_map(meta) do
    :telemetry.execute([:xylon, :dc, event], measurements, meta)
    :ok
  end
end
