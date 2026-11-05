defmodule Xylon.Datachannel.ChannelBufferTest do
  use ExUnit.Case, async: false

  alias Xylon.Datachannel.{Backend, ChannelBuffer}
  alias Xylon.{Participant, Session}

  test "skip mode does not enqueue" do
    session = %Session{id: "s1", tenant_id: "t1"}

    assert {:ok, state} =
             Backend.init(session,
               mode: :fanout_except_sender,
               on_no_channel: :skip,
               labels: ["chat"]
             )

    a = %Participant{id: "a", peer_ref: self()}
    b = %Participant{id: "b", peer_ref: self()}
    assert {:ok, state} = Backend.participant_joined(a, state)
    assert {:ok, state} = Backend.participant_joined(b, state)

    ref_a = make_ref()
    assert {:ok, state} = Backend.handle_datachannel_open(a, %{ref: ref_a, label: "chat", id: 0}, state)

    assert {:forward_dc, [], _} = Backend.handle_datachannel(a, ref_a, "hello", state)
    assert ChannelBuffer.peek(state.plan_ref, "b", "chat") == []
  end

  test "buffer enqueues missing dest and flushes on open" do
    session = %Session{id: "s1", tenant_id: "t1"}

    assert {:ok, state} =
             Backend.init(session,
               mode: :fanout_except_sender,
               on_no_channel: :buffer,
               labels: ["chat"]
             )

    a = %Participant{id: "a", peer_ref: self()}
    b = %Participant{id: "b", peer_ref: self()}
    assert {:ok, state} = Backend.participant_joined(a, state)
    assert {:ok, state} = Backend.participant_joined(b, state)

    ref_a = make_ref()
    assert {:ok, state} = Backend.handle_datachannel_open(a, %{ref: ref_a, label: "chat", id: 0}, state)

    assert {:forward_dc, [], state} = Backend.handle_datachannel(a, ref_a, "hello", state)
    assert ["hello"] = ChannelBuffer.peek(state.plan_ref, "b", "chat")

    ref_b = make_ref()

    assert {:forward_dc, [{^b, ^ref_b, "hello"}], state} =
             Backend.handle_datachannel_open(b, %{ref: ref_b, label: "chat", id: 1}, state)

    assert ChannelBuffer.peek(state.plan_ref, "b", "chat") == []
  end

  test "overflow drops oldest and emits telemetry" do
    parent = self()
    handler_id = "buf-overflow-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:xylon, :dc, :drop],
        fn _e, _m, meta, _c -> send(parent, {:dc_drop, meta}) end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    plan = make_ref()
    ChannelBuffer.ensure_table!()
    meta = %{session_id: "s", tenant_id: "t"}

    Enum.each(1..9, fn i ->
      assert :ok = ChannelBuffer.push(plan, "b", "chat", "m#{i}", meta)
    end)

    assert_receive {:dc_drop, %{reason: :buffer_overflow}}, 200
    payloads = ChannelBuffer.peek(plan, "b", "chat")
    assert length(payloads) == 8
    assert hd(payloads) == "m2"
    assert List.last(payloads) == "m9"
  end

  test "buffer does not emit :skip_no_channel for buffered dests" do
    parent = self()
    handler_id = "buf-skip-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:xylon, :dc, :skip_no_channel],
        fn _e, _m, meta, _c -> send(parent, {:dc_skip, meta}) end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    session = %Session{id: "s1", tenant_id: "t1"}

    assert {:ok, state} =
             Backend.init(session,
               mode: :fanout_except_sender,
               on_no_channel: :buffer,
               labels: ["chat"]
             )

    a = %Participant{id: "a", peer_ref: self()}
    b = %Participant{id: "b", peer_ref: self()}
    assert {:ok, state} = Backend.participant_joined(a, state)
    assert {:ok, state} = Backend.participant_joined(b, state)

    ref_a = make_ref()
    assert {:ok, state} = Backend.handle_datachannel_open(a, %{ref: ref_a, label: "chat", id: 0}, state)

    assert {:forward_dc, [], _} = Backend.handle_datachannel(a, ref_a, "hello", state)
    refute_receive {:dc_skip, _}, 50
  end
end
