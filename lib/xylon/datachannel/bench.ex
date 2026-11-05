defmodule Xylon.Datachannel.Bench do
  @moduledoc """
  FakePeer fanout latency harness for the datachannel Backend.

  ## What problem this solves

  Operators need in-node forward cost (not browser RTT) for N-peer chat fanout
  and same-VM Pipe cascade density before claiming readiness. This module builds
  Session + Forwarder with instrumented FakePeers, injects stamped payloads at a
  fixed pps, and reports p50/p99 plus Session/Forwarder/Peer mailbox lengths.
  Default `:dc_passthrough` uses the DC SendHandle hot path; `:async_peer` omits
  passthrough so Forwarder casts into FakePeer (gates Phase 12 Peer pressure).
  Cascade modes (`:cascade_pipe`, `:cascade_subset`) wire two Sessions through
  `Manager.connect_pair` and sample far-edge egress only. During the inject
  window the harness raises Logger to `:warning` so per-message route/fanout
  debug logs do not dominate stamp p99.

  ## Internal

  Lab / operator harness. Not part of the attach/publish API.

  ## RFCs

  - Observability only; transport is [RFC 8831](https://www.rfc-editor.org/rfc/rfc8831)
  """

  alias XGW.Trunk.Pipe
  alias Xylon.{Forwarder, Participant, Session}
  alias Xylon.Datachannel
  alias Xylon.Trunk.{Bridge, Manager}

  @master_key <<0xE1F97A0D3E018BE0D64FA32C06DE4139::size(128)>>
  @master_salt <<0x0EC675AD498AFEEBB6960B3AABE6::size(112)>>

  @cascade_modes [:cascade_pipe, :cascade_subset]
  @single_modes [:fanout_except_sender, :fanout_subset]

  @doc """
  Runs a datachannel fanout or cascade bench and returns a report map.

  Opts: `:n` (peers, default 20; cascade = far-edge peer count), `:pps`
  (aggregate inject rate, default 200), `:duration_s` (default 2.0), `:hw`,
  `:publishers` (single-node only), `:send_path` (`:dc_passthrough` default, or
  `:async_peer`; cascade is passthrough only), `:mode`
  (`:fanout_except_sender` | `:fanout_subset` | `:cascade_pipe` |
  `:cascade_subset`), `:subset_k` (single-node `1..n-1`; cascade `1..n`),
  `:label` (`"chat"` | `"state"`).
  """
  @spec run(keyword()) :: {:ok, map()} | {:error, term()}
  def run(opts \\ []) do
    n = Keyword.get(opts, :n, 20)
    pps = Keyword.get(opts, :pps, 200)
    duration_s = Keyword.get(opts, :duration_s, 2.0) * 1.0
    hw = Keyword.get(opts, :hw, "unknown")
    publishers = Keyword.get(opts, :publishers, 1)
    send_path = Keyword.get(opts, :send_path, :dc_passthrough)
    mode = Keyword.get(opts, :mode, :fanout_except_sender)
    subset_k = Keyword.get(opts, :subset_k)
    label = Keyword.get(opts, :label, "chat")

    cond do
      n < 2 or pps < 1 or duration_s <= 0 ->
        {:error, :bad_opts}

      mode not in @single_modes ++ @cascade_modes ->
        {:error, :bad_opts}

      label not in ["chat", "state"] ->
        {:error, :bad_opts}

      mode in @cascade_modes and send_path != :dc_passthrough ->
        {:error, :bad_opts}

      mode == :fanout_subset and
          (not is_integer(subset_k) or subset_k < 1 or subset_k > n - 1) ->
        {:error, :bad_opts}

      mode == :cascade_subset and
          (not is_integer(subset_k) or subset_k < 1 or subset_k > n) ->
        {:error, :bad_opts}

      mode == :fanout_except_sender and
          (publishers < 1 or publishers > n) ->
        {:error, :bad_opts}

      true ->
        :ok = Datachannel.attach()

        cfg = %{
          n: n,
          pps: pps,
          duration_s: duration_s,
          hw: hw,
          publishers: publishers,
          send_path: send_path,
          mode: mode,
          subset_k: subset_k,
          label: label
        }

        if mode in @cascade_modes do
          do_cascade_run(cfg)
        else
          do_run(cfg)
        end
    end
  end

  @doc "Formats a report for stdout."
  @spec format(map()) :: String.t()
  def format(report) when is_map(report) do
    pipe =
      case Map.get(report, :pipe_udp_out_delta) do
        nil -> ""
        d -> "\n  pipe_udp_out_delta=#{d}"
      end

    """
    datachannel bench
      hw=#{report.hw} n=#{report.n} mode=#{report.mode} subset_k=#{inspect(report.subset_k)} label=#{report.label}
      publishers=#{report.publishers} pps=#{report.configured_pps} duration_s=#{report.duration_s}
      samples=#{report.sample_count} injects=#{report.injects} out_sends=#{report.out_sends}
      p50_ms=#{report.p50_ms} p99_ms=#{report.p99_ms}
      session_mailbox_max=#{report.session_mailbox_max} forwarder_mailbox_max=#{report.forwarder_mailbox_max} peer_mailbox_max=#{report.peer_mailbox_max}
      send_path=#{report.send_path} stamp_plane=#{inspect(report.stamp_plane)}#{pipe}
    """
  end

  @doc "JSON-encodes a report."
  @spec to_json(map()) :: String.t()
  def to_json(report) when is_map(report) do
    Jason.encode!(report)
  end

  @doc """
  Phase 12 gate decisions from harness reports.

  `async_report` must use `send_path: :async_peer` (T12.4 Peer mailbox / p99).
  `fwd_report` is typically N=50 multi-pub passthrough (T12.5 Forwarder mailbox).
  Optional `:async_baseline_p99_ms` is N=20 async p99 for the 2x regression check.
  """
  @spec phase12_gates(map(), map(), keyword()) :: map()
  def phase12_gates(async_report, fwd_report, opts \\ []) do
    baseline = Keyword.get(opts, :async_baseline_p99_ms)
    peer_max = Map.get(async_report, :peer_mailbox_max, 0)
    async_p99 = Map.get(async_report, :p99_ms)
    fwd_max = Map.get(fwd_report, :forwarder_mailbox_max, 0)
    fwd_p99 = Map.get(fwd_report, :p99_ms)
    session_max = Map.get(fwd_report, :session_mailbox_max, 0)

    peer_pressure = peer_max > 50

    peer_p99_fail =
      is_number(baseline) and is_number(async_p99) and baseline > 0 and async_p99 > baseline * 2

    fwd_pressure = fwd_max > 100

    fwd_p99_fail =
      is_number(baseline) and is_number(fwd_p99) and session_max == 0 and baseline > 0 and
        fwd_p99 > baseline * 2

    %{
      t12_4_implement?: peer_pressure or peer_p99_fail,
      t12_5_implement?: fwd_pressure or fwd_p99_fail,
      peer_mailbox_max: peer_max,
      async_p99_ms: async_p99,
      async_baseline_p99_ms: baseline,
      forwarder_mailbox_max: fwd_max,
      forwarder_p99_ms: fwd_p99,
      session_mailbox_max: session_max
    }
  end

  @doc "Builds an 8-byte big-endian nanosecond stamp."
  @spec stamp_bin(integer()) :: binary()
  def stamp_bin(ns) when is_integer(ns), do: <<ns::signed-big-integer-size(64)>>

  @doc "Reads stamp prefix; `:error` when too short."
  @spec read_stamp(binary()) :: {:ok, integer()} | :error
  def read_stamp(<<ns::signed-big-integer-size(64), _::binary>>), do: {:ok, ns}
  def read_stamp(_), do: :error

  defp do_run(%{
         n: n,
         pps: pps,
         duration_s: duration_s,
         hw: hw,
         publishers: publishers,
         send_path: send_path,
         mode: mode,
         subset_k: subset_k,
         label: label
       }) do
    collector = :ets.new(:xylon_dc_bench_lat, [:public, :duplicate_bag])
    mailbox_acc = new_mailbox_acc()

    sid = "dc-bench-#{System.unique_integer([:positive])}"

    {session_opts, inject_publishers, report_publishers} =
      case mode do
        :fanout_subset ->
          subset_ids = Enum.map(2..(subset_k + 1), fn i -> "p#{i}" end)

          {[mode: :fanout_subset, subset: subset_ids, labels: [label], default_label: label], 1, 1}

        :fanout_except_sender ->
          {[mode: :fanout_except_sender, labels: [label], default_label: label], publishers,
           publishers}
      end

    with {:ok, session} <- Session.create("bench", sid, :datachannel, session_opts) do
      fwd = Session.forwarder(session)

      peers =
        Enum.map(1..n, fn i ->
          id = "p#{i}"
          {:ok, peer} = __MODULE__.FakePeer.start_link(collector: collector, send_path: send_path)
          ch = make_ref()
          participant = %Participant{id: id, peer_ref: peer, role: :publisher}
          {id, peer, ch, participant}
        end)

      :sys.replace_state(session, fn state ->
        {participants, backend_state} =
          Enum.reduce(peers, {%{}, state.backend_state}, fn {id, _peer, _ch, p}, {map, bs} ->
            {:ok, bs} = state.backend.participant_joined(p, bs)
            {Map.put(map, id, p), bs}
          end)

        %{state | participants: participants, backend_state: backend_state}
      end)

      Enum.each(peers, fn {_id, _peer, _ch, p} ->
        :ok = Forwarder.install_echo(fwd, p)
      end)

      Enum.with_index(peers, 1)
      |> Enum.each(fn {{_id, peer, ch, _p}, idx} ->
        send(session, {:xgw, :datachannel, peer, %{ref: ch, label: label, id: idx}})
      end)

      Process.sleep(30)

      senders = Enum.take(peers, inject_publishers)
      interval_ms = max(1, div(1000, pps))
      deadline = System.monotonic_time(:millisecond) + trunc(duration_s * 1000)
      peer_pids = Enum.map(peers, fn {_id, peer, _ch, _} -> peer end)

      {injects, _} =
        with_quiet_logs(fn ->
          injects =
            inject_loop(fwd, senders, interval_ms, deadline, 0, 0, fn ->
              sample_mailboxes(mailbox_acc, session, fwd, peer_pids)
            end)

          Enum.each(1..20, fn _ ->
            sample_mailboxes(mailbox_acc, session, fwd, peer_pids)
            Process.sleep(5)
          end)

          {injects, :ok}
        end)

      report =
        finish_report(%{
          hw: hw,
          mode: mode,
          subset_k: subset_k,
          n: n,
          publishers: report_publishers,
          duration_s: duration_s,
          pps: pps,
          injects: injects,
          send_path: send_path,
          label: label,
          collector: collector,
          mailbox_acc: mailbox_acc
        })

      Enum.each(peers, fn {_id, peer, _ch, _} -> GenServer.stop(peer, :normal) end)
      :ets.delete(collector)
      :ets.delete(mailbox_acc)
      Process.exit(session, :kill)

      {:ok, report}
    end
  end

  defp do_cascade_run(%{
         n: n,
         pps: pps,
         duration_s: duration_s,
         hw: hw,
         mode: mode,
         subset_k: subset_k,
         label: label
       }) do
    collector = :ets.new(:xylon_dc_bench_lat, [:public, :duplicate_bag])
    discard = :ets.new(:xylon_dc_bench_discard, [:public, :duplicate_bag])
    mailbox_acc = new_mailbox_acc()

    uid = System.unique_integer([:positive])
    sid_a = "dc-casc-a-#{uid}"
    sid_b = "dc-casc-b-#{uid}"

    b_opts =
      case mode do
        :cascade_pipe ->
          [mode: :fanout_except_sender, labels: [label], default_label: label]

        :cascade_subset ->
          subset_ids = Enum.map(1..subset_k, fn i -> "b#{i}" end)

          [mode: :fanout_subset, subset: subset_ids, labels: [label], default_label: label]
      end

    a_opts = [mode: :fanout_except_sender, labels: [label], default_label: label]

    with {:ok, session_a} <- Session.create("bench", sid_a, :datachannel, a_opts),
         {:ok, session_b} <- Session.create("bench", sid_b, :datachannel, b_opts),
         fwd_a = Session.forwarder(session_a),
         fwd_b = Session.forwarder(session_b),
         {:ok, %{a: trunk_a, b: trunk_b}} <-
           Manager.connect_pair(session_a, session_b,
             id_a: "trunk_ab",
             id_b: "trunk_ba",
             master_key: @master_key,
             master_salt: @master_salt
           ),
         {:ok, bridge_a} <- Bridge.start_link(forwarder: fwd_a),
         {:ok, bridge_b} <- Bridge.start_link(forwarder: fwd_b),
         :ok <- Bridge.attach_pipe(bridge_a, trunk_a.peer_ref),
         :ok <- Bridge.attach_pipe(bridge_b, trunk_b.peer_ref) do
      {:ok, peer_a} =
        __MODULE__.FakePeer.start_link(collector: discard, send_path: :dc_passthrough)

      ch_a = make_ref()
      pub_a = %Participant{id: "a", peer_ref: peer_a, role: :publisher}

      far_peers =
        Enum.map(1..n, fn i ->
          id = "b#{i}"
          {:ok, peer} = __MODULE__.FakePeer.start_link(collector: collector, send_path: :dc_passthrough)
          ch = make_ref()
          participant = %Participant{id: id, peer_ref: peer, role: :publisher}
          {id, peer, ch, participant}
        end)

      :sys.replace_state(session_a, fn state ->
        {:ok, bs} = state.backend.participant_joined(pub_a, state.backend_state)
        {:ok, bs} = state.backend.participant_joined(trunk_a, bs)

        %{
          state
          | participants: %{"a" => pub_a, "trunk_ab" => trunk_a},
            backend_state: bs
        }
      end)

      :sys.replace_state(session_b, fn state ->
        {participants, bs} =
          Enum.reduce(far_peers, {%{"trunk_ba" => trunk_b}, state.backend_state}, fn {id, _, _, p},
                                                                                     {map, bs} ->
            {:ok, bs} = state.backend.participant_joined(p, bs)
            {Map.put(map, id, p), bs}
          end)

        {:ok, bs} = state.backend.participant_joined(trunk_b, bs)

        %{state | participants: participants, backend_state: bs}
      end)

      :ok = Forwarder.install_echo(fwd_a, pub_a)
      :ok = Forwarder.install_echo(fwd_a, trunk_a)
      :ok = Forwarder.install_echo(fwd_b, trunk_b)

      Enum.each(far_peers, fn {_id, _peer, _ch, p} ->
        :ok = Forwarder.install_echo(fwd_b, p)
      end)

      {:ok, ch_ta} = Pipe.ensure_label(trunk_a.peer_ref, label)
      {:ok, ch_tb} = Pipe.ensure_label(trunk_b.peer_ref, label)

      send(session_a, {:xgw, :datachannel, peer_a, %{ref: ch_a, label: label, id: 1}})
      send(session_a, {:xgw, :datachannel, trunk_a.peer_ref, %{ref: ch_ta, label: label, id: 2}})

      Enum.with_index(far_peers, 3)
      |> Enum.each(fn {{_id, peer, ch, _p}, idx} ->
        send(session_b, {:xgw, :datachannel, peer, %{ref: ch, label: label, id: idx}})
      end)

      send(
        session_b,
        {:xgw, :datachannel, trunk_b.peer_ref, %{ref: ch_tb, label: label, id: 2 + n + 1}}
      )

      Process.sleep(40)

      before_stats = Pipe.stats(trunk_a.peer_ref)
      interval_ms = max(1, div(1000, pps))
      deadline = System.monotonic_time(:millisecond) + trunc(duration_s * 1000)
      far_pids = Enum.map(far_peers, fn {_id, peer, _ch, _} -> peer end)

      senders = [{"a", peer_a, ch_a, pub_a}]

      {injects, pipe_delta} =
        with_quiet_logs(fn ->
          injects =
            inject_loop(fwd_a, senders, interval_ms, deadline, 0, 0, fn ->
              sample_mailboxes(mailbox_acc, session_a, fwd_a, [peer_a])
              sample_mailboxes(mailbox_acc, session_b, fwd_b, far_pids)
            end)

          Enum.each(1..20, fn _ ->
            sample_mailboxes(mailbox_acc, session_a, fwd_a, [peer_a])
            sample_mailboxes(mailbox_acc, session_b, fwd_b, far_pids)
            Process.sleep(5)
          end)

          after_stats = Pipe.stats(trunk_a.peer_ref)
          {injects, max(0, after_stats.udp_out - before_stats.udp_out)}
        end)

      report =
        finish_report(%{
          hw: hw,
          mode: mode,
          subset_k: subset_k,
          n: n,
          publishers: 1,
          duration_s: duration_s,
          pps: pps,
          injects: injects,
          send_path: :dc_passthrough,
          label: label,
          collector: collector,
          mailbox_acc: mailbox_acc,
          pipe_udp_out_delta: pipe_delta
        })

      GenServer.stop(peer_a, :normal)
      Enum.each(far_peers, fn {_id, peer, _ch, _} -> GenServer.stop(peer, :normal) end)
      GenServer.stop(bridge_a, :normal)
      GenServer.stop(bridge_b, :normal)
      :ets.delete(collector)
      :ets.delete(discard)
      :ets.delete(mailbox_acc)
      Process.exit(session_a, :kill)
      Process.exit(session_b, :kill)

      {:ok, report}
    end
  end

  defp finish_report(opts) do
    latencies =
      :ets.tab2list(opts.collector)
      |> Enum.map(fn {lat} -> lat end)
      |> Enum.sort()

    [{_, session_max}] = :ets.lookup(opts.mailbox_acc, :session_max)
    [{_, fwd_max}] = :ets.lookup(opts.mailbox_acc, :forwarder_max)
    [{_, peer_max}] = :ets.lookup(opts.mailbox_acc, :peer_max)

    base = %{
      hw: opts.hw,
      scenario: Atom.to_string(opts.mode),
      mode: opts.mode,
      subset_k: opts.subset_k,
      n: opts.n,
      publishers: opts.publishers,
      duration_s: Float.round(opts.duration_s, 3),
      configured_pps: opts.pps,
      injects: opts.injects,
      out_sends: length(latencies),
      sample_count: length(latencies),
      p50_ms: ns_to_ms(percentile(latencies, 0.50)),
      p99_ms: ns_to_ms(percentile(latencies, 0.99)),
      session_mailbox_max: session_max,
      forwarder_mailbox_max: fwd_max,
      peer_mailbox_max: peer_max,
      send_path: opts.send_path,
      stamp_plane: :payload,
      label: opts.label
    }

    case Map.get(opts, :pipe_udp_out_delta) do
      nil -> base
      delta -> Map.put(base, :pipe_udp_out_delta, delta)
    end
  end

  defp new_mailbox_acc do
    mailbox_acc = :ets.new(:xylon_dc_bench_mb, [:public, :set])
    :ets.insert(mailbox_acc, {:session_max, 0})
    :ets.insert(mailbox_acc, {:forwarder_max, 0})
    :ets.insert(mailbox_acc, {:peer_max, 0})
    mailbox_acc
  end

  defp with_quiet_logs(fun) when is_function(fun, 0) do
    # Per-message Logger.debug on route/fanout dominates stamp p99 under N=50.
    prev = Logger.level()
    Logger.configure(level: :warning)

    try do
      fun.()
    after
      Logger.configure(level: prev)
    end
  end

  defp inject_loop(fwd, senders, interval_ms, deadline, count, sender_idx, on_tick) do
    now = System.monotonic_time(:millisecond)

    if now >= deadline do
      count
    else
      {_id, peer, ch, _} = Enum.at(senders, rem(sender_idx, length(senders)))
      stamp = System.monotonic_time(:nanosecond)
      payload = stamp_bin(stamp) <> "x"
      send(fwd, {:xgw, :datachannel_message, peer, ch, payload})
      on_tick.()
      Process.sleep(interval_ms)
      inject_loop(fwd, senders, interval_ms, deadline, count + 1, sender_idx + 1, on_tick)
    end
  end

  defp sample_mailboxes(acc, session, fwd, peer_pids) do
    bump(acc, :session_max, mailbox_len(session))
    bump(acc, :forwarder_max, mailbox_len(fwd))

    peer_max =
      peer_pids
      |> Enum.map(&mailbox_len/1)
      |> Enum.max(fn -> 0 end)

    bump(acc, :peer_max, peer_max)
  end

  defp bump(acc, key, value) do
    [{^key, prev}] = :ets.lookup(acc, key)
    if value > prev, do: :ets.insert(acc, {key, value})
    :ok
  end

  defp mailbox_len(pid) when is_pid(pid) do
    case Process.info(pid, :message_queue_len) do
      {:message_queue_len, n} -> n
      _ -> 0
    end
  end

  defp percentile([], _), do: nil

  defp percentile(sorted, p) when is_list(sorted) do
    n = length(sorted)
    idx = min(n - 1, max(0, trunc(Float.ceil(p * n) - 1)))
    Enum.at(sorted, idx)
  end

  defp ns_to_ms(nil), do: nil
  defp ns_to_ms(ns), do: Float.round(ns / 1_000_000, 4)

  defmodule FakePeer do
    @moduledoc """
    Instrumented peer stub that records DC send latency for the bench harness.

    ## What problem this solves

    Real XGW Peers are out of scope for the in-node fanout bench. FakePeer
    publishes a DC SendHandle passthrough so Forwarder records latency in
    `hot_send_dc/3` without a Peer GenServer hop (same idea as RTP passthrough).

    ## RFCs

    - Application bench only; no wire RFC
    """

    use GenServer

    alias XGW.Peer.SendHandle

    @doc "Starts an instrumented FakePeer. Opt `:collector` is an ETS table."
    @spec start_link(keyword()) :: GenServer.on_start()
    def start_link(opts \\ []) do
      GenServer.start_link(__MODULE__, opts)
    end

    @doc """
    Synchronous DC SendHandle passthrough callback (no GenServer hop in the sample).

    `collector` is the ETS tid passed as the MFA sink. `type` is the PPI
    (`:string` | `:binary`) from Forwarder; ignored for latency samples.
    """
    @spec hot_send_dc(reference(), reference(), binary(), :string | :binary) :: :ok
    def hot_send_dc(collector, _ref, data, _type \\ :string) when is_binary(data) do
      record_into(collector, data)
      :ok
    end

    @impl true
    def init(opts) do
      collector = Keyword.fetch!(opts, :collector)
      send_path = Keyword.get(opts, :send_path, :dc_passthrough)
      SendHandle.ensure_table!()

      case send_path do
        :async_peer ->
          _ = SendHandle.invalidate(self())

        _ ->
          :ok = SendHandle.publish_dc_passthrough(self(), {__MODULE__, :hot_send_dc, collector})
      end

      {:ok, %{collector: collector}}
    end

    @impl true
    def terminate(_reason, _state) do
      _ = SendHandle.invalidate(self())
      :ok
    end

    @impl true
    def handle_call({:send_datachannel, _ref, data}, _from, state) when is_binary(data) do
      record_into(state.collector, data)
      {:reply, :ok, state}
    end

    def handle_call({:send_datachannel, ref, data, _type}, from, state) when is_binary(data) do
      handle_call({:send_datachannel, ref, data}, from, state)
    end

    @impl true
    def handle_cast({:send_datachannel, _ref, data}, state) when is_binary(data) do
      record_into(state.collector, data)
      {:noreply, state}
    end

    def handle_cast({:send_datachannel, ref, data, _type}, state) when is_binary(data) do
      handle_cast({:send_datachannel, ref, data}, state)
    end

    defp record_into(collector, data) do
      case Xylon.Datachannel.Bench.read_stamp(data) do
        {:ok, stamp_ns} ->
          lat = System.monotonic_time(:nanosecond) - stamp_ns
          true = :ets.insert(collector, {lat})

        :error ->
          :ok
      end
    end
  end
end
