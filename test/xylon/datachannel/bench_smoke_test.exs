defmodule Xylon.Datachannel.BenchSmokeTest do
  use ExUnit.Case, async: false

  alias Xylon.Datachannel.Bench

  setup do
    Xylon.Catalogue.reset!()
    :ok
  end

  test "short fanout bench returns latency samples and cold session mailbox" do
    assert {:ok, report} =
             Bench.run(n: 3, pps: 40, duration_s: 0.5, hw: "smoke")

    assert report.n == 3
    assert report.injects > 0
    assert report.sample_count > 0
    assert is_number(report.p50_ms)
    assert is_number(report.p99_ms)
    assert report.session_mailbox_max == 0
    assert report.send_path == :dc_passthrough
    assert report.stamp_plane == :payload
    assert is_integer(report.peer_mailbox_max)
    assert report.publishers == 1
  end

  test "async_peer path samples peer mailbox without passthrough" do
    assert {:ok, report} =
             Bench.run(
               n: 3,
               pps: 40,
               duration_s: 0.4,
               publishers: 2,
               send_path: :async_peer,
               hw: "smoke-async"
             )

    assert report.send_path == :async_peer
    assert report.publishers == 2
    assert report.sample_count > 0
  end

  test "phase12_gates WONTFIX when mailboxes stay under thresholds" do
    async = %{peer_mailbox_max: 2, p99_ms: 1.0}
    fwd = %{forwarder_mailbox_max: 5, p99_ms: 0.5, session_mailbox_max: 0}
    gates = Bench.phase12_gates(async, fwd, async_baseline_p99_ms: 0.8)
    refute gates.t12_4_implement?
    refute gates.t12_5_implement?
  end

  test "rejects bad opts" do
    assert {:error, :bad_opts} = Bench.run(n: 1, pps: 10, duration_s: 1.0)
    assert {:error, :bad_opts} = Bench.run(n: 5, mode: :fanout_subset, subset_k: 5)
    assert {:error, :bad_opts} = Bench.run(n: 5, mode: :fanout_subset)
  end

  test "subset_k fanout returns samples with mode and label" do
    assert {:ok, report} =
             Bench.run(
               n: 5,
               pps: 40,
               duration_s: 0.4,
               mode: :fanout_subset,
               subset_k: 2,
               label: "state",
               hw: "smoke-subset"
             )

    assert report.mode == :fanout_subset
    assert report.subset_k == 2
    assert report.label == "state"
    assert report.publishers == 1
    assert report.sample_count > 0
  end
end
