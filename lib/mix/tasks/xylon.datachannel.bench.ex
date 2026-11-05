defmodule Mix.Tasks.Xylon.Datachannel.Bench do
  @shortdoc "Run datachannel fanout / Pipe cascade FakePeer latency bench"

  @moduledoc """
  Runs the N-peer datachannel forward latency harness (single-node fanout or
  same-VM Pipe cascade).

  ## What problem this solves

  Operators need a single Mix entry for in-node DC fanout cost and same-VM Pipe
  cascade density without wiring FakePeers by hand. See `Xylon.Datachannel.Bench`.

  ## Examples

      XTURN_LOCAL_DEPS=1 mix xylon.datachannel.bench --n 20 --pps 200 --duration 30 --hw "m1-pro"
      XTURN_LOCAL_DEPS=1 mix xylon.datachannel.bench --n 50 --pps 200 --duration 5 --mode fanout_subset --subset-k 10 --label state --json
      XTURN_LOCAL_DEPS=1 mix xylon.datachannel.bench --mode cascade_pipe --n 50 --pps 200 --duration 5 --hw "m1-pro"
      XTURN_LOCAL_DEPS=1 mix xylon.datachannel.bench --mode cascade_subset --n 50 --subset-k 10 --pps 200 --duration 5 --label chat --hw "m1-pro"

  ## RFCs

  None for the Mix task; the scenario exercises [RFC 8831](https://www.rfc-editor.org/rfc/rfc8831) paths.
  """

  use Mix.Task

  alias Xylon.Datachannel.Bench

  @switches [
    n: :integer,
    pps: :integer,
    duration: :float,
    hw: :string,
    json: :boolean,
    publishers: :integer,
    send_path: :string,
    mode: :string,
    subset_k: :integer,
    label: :string
  ]

  @impl Mix.Task
  def run(argv) do
    {opts, _, _} = OptionParser.parse(argv, strict: @switches)

    Mix.Task.run("app.start")

    Application.put_env(:xylon, :ice, advertised_ip: {127, 0, 0, 1})

    send_path =
      case opts[:send_path] do
        "async_peer" -> :async_peer
        "dc_passthrough" -> :dc_passthrough
        nil -> :dc_passthrough
        other -> Mix.raise("unknown --send-path #{inspect(other)} (dc_passthrough|async_peer)")
      end

    mode =
      case opts[:mode] do
        "fanout_subset" -> :fanout_subset
        "fanout_except_sender" -> :fanout_except_sender
        "cascade_pipe" -> :cascade_pipe
        "cascade_subset" -> :cascade_subset
        nil -> :fanout_except_sender
        other -> Mix.raise("unknown --mode #{inspect(other)}")
      end

    bench_opts =
      []
      |> put(:n, opts[:n])
      |> put(:pps, opts[:pps])
      |> put(:duration_s, opts[:duration])
      |> put(:hw, opts[:hw] || default_hw())
      |> put(:publishers, opts[:publishers])
      |> put(:send_path, send_path)
      |> put(:mode, mode)
      |> put(:subset_k, opts[:subset_k])
      |> put(:label, opts[:label])

    case Bench.run(bench_opts) do
      {:ok, report} ->
        if opts[:json] do
          Mix.shell().info(Bench.to_json(report))
        else
          Mix.shell().info(Bench.format(report))
        end

      {:error, reason} ->
        Mix.raise("datachannel bench failed: #{inspect(reason)}")
    end
  end

  defp put(kw, _key, nil), do: kw
  defp put(kw, key, value), do: Keyword.put(kw, key, value)

  defp default_hw do
    cores =
      case :erlang.system_info(:logical_processors_available) do
        n when is_integer(n) and n > 0 -> n
        _ -> :erlang.system_info(:logical_processors)
      end

    "local-#{cores}c"
  end
end
