defmodule Xylon.Datachannel.BenchCascadeSmokeTest do
  use ExUnit.Case, async: false

  alias Xylon.Datachannel.Bench

  setup do
    Xylon.Catalogue.reset!()
    :ok
  end

  test "cascade_pipe returns far-edge samples and pipe_udp_out_delta" do
    assert {:ok, report} =
             Bench.run(
               mode: :cascade_pipe,
               n: 4,
               pps: 100,
               duration_s: 0.5,
               hw: "ci-smoke"
             )

    assert report.mode == :cascade_pipe
    assert report.scenario == "cascade_pipe"
    assert report.stamp_plane == :payload
    assert report.send_path == :dc_passthrough
    assert report.n == 4
    assert report.injects > 0
    assert report.sample_count > 0
    assert report.sample_count <= report.injects * 4
    assert is_number(report.p50_ms)
    assert is_number(report.p99_ms)
    assert is_integer(report.session_mailbox_max)
    assert is_integer(report.forwarder_mailbox_max)
    assert is_integer(report.peer_mailbox_max)
    assert report.pipe_udp_out_delta > 0
  end

  test "cascade_subset samples K far receivers not n" do
    assert {:ok, report} =
             Bench.run(
               mode: :cascade_subset,
               n: 5,
               subset_k: 2,
               pps: 100,
               duration_s: 0.5,
               hw: "ci-smoke"
             )

    assert report.mode == :cascade_subset
    assert report.subset_k == 2
    assert report.stamp_plane == :payload
    assert report.injects > 0
    assert report.sample_count > 0
    assert report.sample_count <= report.injects * 2
    # Must not look like full N=5 fanout
    assert report.sample_count < report.injects * 5
    assert report.pipe_udp_out_delta > 0
  end

  test "cascade_subset rejects subset_k above n" do
    assert {:error, :bad_opts} =
             Bench.run(mode: :cascade_subset, n: 5, subset_k: 6, pps: 10, duration_s: 0.2)
  end
end
