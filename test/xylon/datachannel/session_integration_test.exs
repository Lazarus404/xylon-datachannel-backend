defmodule Xylon.Datachannel.SessionIntegrationTest do
  use ExUnit.Case, async: false

  alias Xylon.{Forwarder, Participant, Session}
  alias Xylon.Datachannel
  alias Xylon.Datachannel.Plan

  defmodule FakePeer do
    @moduledoc false
    use GenServer

    def start_link, do: GenServer.start_link(__MODULE__, [])
    def drain(pid), do: GenServer.call(pid, :drain)
    def drain_modes(pid), do: GenServer.call(pid, :drain_modes)

    @impl true
    def init(_), do: {:ok, %{events: [], modes: []}}

    @impl true
    def handle_call({:send_datachannel, ref, data}, _from, state) do
      handle_call({:send_datachannel, ref, data, :string}, nil, state)
    end

    def handle_call({:send_datachannel, ref, data, type}, _from, state) do
      {:reply, :ok,
       %{
         state
         | events: [{:send_datachannel, ref, data, type} | state.events],
           modes: [:call | state.modes]
       }}
    end

    def handle_call(:drain, _from, state) do
      {:reply, Enum.reverse(state.events), %{state | events: []}}
    end

    def handle_call(:drain_modes, _from, state) do
      {:reply, Enum.reverse(state.modes), %{state | modes: []}}
    end

    @impl true
    def handle_cast({:send_datachannel, ref, data}, state) do
      handle_cast({:send_datachannel, ref, data, :string}, state)
    end

    def handle_cast({:send_datachannel, ref, data, type}, state) do
      {:noreply,
       %{
         state
         | events: [{:send_datachannel, ref, data, type} | state.events],
           modes: [:cast | state.modes]
       }}
    end
  end

  setup do
    Xylon.Catalogue.reset!()
    assert :ok = Datachannel.attach()
    :ok
  end

  test "Forwarder DC fanout without SendHandle uses async cast not call" do
    assert {:ok, session} =
             Session.create("t1", "dc-cast", :datachannel, mode: :fanout_except_sender)

    fwd = Session.forwarder(session)
    assert {:ok, peer_a} = FakePeer.start_link()
    assert {:ok, peer_b} = FakePeer.start_link()
    ch_a = make_ref()
    ch_b = make_ref()

    a = %Participant{id: "a", peer_ref: peer_a, role: :publisher}
    b = %Participant{id: "b", peer_ref: peer_b, role: :publisher}

    :sys.replace_state(session, fn state ->
      {:ok, backend_state} = state.backend.participant_joined(a, state.backend_state)
      {:ok, backend_state} = state.backend.participant_joined(b, backend_state)

      %{
        state
        | participants: %{"a" => a, "b" => b},
          backend_state: backend_state
      }
    end)

    assert :ok = Forwarder.install_echo(fwd, a)
    assert :ok = Forwarder.install_echo(fwd, b)

    send(session, {:xgw, :datachannel, peer_a, %{ref: ch_a, label: "chat", id: 1}})
    send(session, {:xgw, :datachannel, peer_b, %{ref: ch_b, label: "chat", id: 2}})
    Process.sleep(30)

    # No DC SendHandle row -> Forwarder falls back to send_datachannel_async.
    _ = XGW.Peer.SendHandle.invalidate(peer_b)

    send(fwd, {:xgw, :datachannel_message, peer_a, ch_a, "via-cast"})

    assert wait_drain(peer_b, [{:send_datachannel, ch_b, "via-cast", :string}])
    assert FakePeer.drain_modes(peer_b) == [:cast]
    assert FakePeer.drain(peer_a) == []
  end

  test "Session and Forwarder Backend.init share one plan_ref" do
    assert {:ok, session} = Session.create("t1", "dc-share", :datachannel)
    fwd = Session.forwarder(session)
    session_plan = Session.plan_ref(session)
    fwd_plan = :sys.get_state(fwd).backend_state.plan_ref
    assert session_plan == fwd_plan
    assert session_plan == session
  end

  test "Forwarder backend_state inherits Session create opts" do
    assert {:ok, session} =
             Session.create("t1", "dc-opts", :datachannel,
               mode: :fanout_all,
               default_label: "room",
               labels: ["room"]
             )

    fwd = Session.forwarder(session)
    session_bs = :sys.get_state(session).backend_state
    fwd_bs = :sys.get_state(fwd).backend_state

    assert session_bs.mode == :fanout_all
    assert fwd_bs.mode == :fanout_all
    assert fwd_bs.default_label == "room"
    assert fwd_bs.labels == ["room"]
  end

  test "fanout_all via Forwarder delivers to every peer including matching labels" do
    assert {:ok, session} =
             Session.create("t1", "dc-all", :datachannel, mode: :fanout_all)

    fwd = Session.forwarder(session)
    assert {:ok, peer_a} = FakePeer.start_link()
    assert {:ok, peer_b} = FakePeer.start_link()
    ch_a = make_ref()
    ch_b = make_ref()

    a = %Participant{id: "a", peer_ref: peer_a, role: :publisher}
    b = %Participant{id: "b", peer_ref: peer_b, role: :publisher}

    :sys.replace_state(session, fn state ->
      {:ok, backend_state} = state.backend.participant_joined(a, state.backend_state)
      {:ok, backend_state} = state.backend.participant_joined(b, backend_state)

      %{
        state
        | participants: %{"a" => a, "b" => b},
          backend_state: backend_state
      }
    end)

    assert :ok = Forwarder.install_echo(fwd, a)
    assert :ok = Forwarder.install_echo(fwd, b)

    send(session, {:xgw, :datachannel, peer_a, %{ref: ch_a, label: "chat", id: 1}})
    send(session, {:xgw, :datachannel, peer_b, %{ref: ch_b, label: "chat", id: 2}})
    Process.sleep(20)

    send(fwd, {:xgw, :datachannel_message, peer_a, ch_a, "broadcast"})

    assert wait_drain(peer_a, [{:send_datachannel, ch_a, "broadcast", :string}])
    assert wait_drain(peer_b, [{:send_datachannel, ch_b, "broadcast", :string}])
  end

  test "two peers: A sends on chat via Forwarder, B receives on B's channel ref" do
    assert {:ok, session} =
             Session.create("t1", "dc-fanout", :datachannel, mode: :fanout_except_sender)

    fwd = Session.forwarder(session)
    assert {:ok, peer_a} = FakePeer.start_link()
    assert {:ok, peer_b} = FakePeer.start_link()
    ch_a = make_ref()
    ch_b = make_ref()

    a = %Participant{id: "a", peer_ref: peer_a, role: :publisher}
    b = %Participant{id: "b", peer_ref: peer_b, role: :publisher}

    :sys.replace_state(session, fn state ->
      {:ok, backend_state} = state.backend.participant_joined(a, state.backend_state)
      {:ok, backend_state} = state.backend.participant_joined(b, backend_state)

      %{
        state
        | participants: %{"a" => a, "b" => b},
          backend_state: backend_state
      }
    end)

    assert :ok = Forwarder.install_echo(fwd, a)
    assert :ok = Forwarder.install_echo(fwd, b)

    send(session, {:xgw, :datachannel, peer_a, %{ref: ch_a, label: "chat", id: 1}})
    send(session, {:xgw, :datachannel, peer_b, %{ref: ch_b, label: "chat", id: 2}})
    Process.sleep(20)

    assert Plan.label_for_ref(session, "a", ch_a) == "chat"
    assert Plan.participant_ids(session) |> Enum.sort() == ["a", "b"]

    send(fwd, {:xgw, :datachannel_message, peer_a, ch_a, "hello"})

    assert wait_drain(peer_b, [{:send_datachannel, ch_b, "hello", :string}])
    assert FakePeer.drain(peer_a) == []
  end

  test "publish/3 injects to all open peers" do
    assert {:ok, session} =
             Session.create("t1", "dc-pub", :datachannel, default_label: "chat")

    assert {:ok, peer_a} = FakePeer.start_link()
    assert {:ok, peer_b} = FakePeer.start_link()
    ch_a = make_ref()
    ch_b = make_ref()

    :sys.replace_state(session, fn state ->
      a = %Participant{id: "a", peer_ref: peer_a, role: :publisher}
      b = %Participant{id: "b", peer_ref: peer_b, role: :publisher}

      {:ok, backend_state} = state.backend.participant_joined(a, state.backend_state)
      {:ok, backend_state} = state.backend.participant_joined(b, backend_state)

      %{
        state
        | participants: %{"a" => a, "b" => b},
          backend_state: backend_state
      }
    end)

    send(session, {:xgw, :datachannel, peer_a, %{ref: ch_a, label: "chat", id: 1}})
    send(session, {:xgw, :datachannel, peer_b, %{ref: ch_b, label: "chat", id: 2}})
    Process.sleep(20)

    assert :ok = Datachannel.publish(session, "server", :all, label: "chat")

    assert wait_drain(peer_a, [{:send_datachannel, ch_a, "server", :string}])
    assert wait_drain(peer_b, [{:send_datachannel, ch_b, "server", :string}])
  end

  test "channel closed removes destination from fanout" do
    assert {:ok, session} =
             Session.create("t1", "dc-close-fanout", :datachannel, mode: :fanout_except_sender)

    fwd = Session.forwarder(session)
    assert {:ok, peer_a} = FakePeer.start_link()
    assert {:ok, peer_b} = FakePeer.start_link()
    ch_a = make_ref()
    ch_b = make_ref()

    a = %Participant{id: "a", peer_ref: peer_a, role: :publisher}
    b = %Participant{id: "b", peer_ref: peer_b, role: :publisher}

    :sys.replace_state(session, fn state ->
      {:ok, backend_state} = state.backend.participant_joined(a, state.backend_state)
      {:ok, backend_state} = state.backend.participant_joined(b, backend_state)

      %{
        state
        | participants: %{"a" => a, "b" => b},
          backend_state: backend_state
      }
    end)

    assert :ok = Forwarder.install_echo(fwd, a)
    assert :ok = Forwarder.install_echo(fwd, b)

    send(session, {:xgw, :datachannel, peer_a, %{ref: ch_a, label: "chat", id: 1}})
    send(session, {:xgw, :datachannel, peer_b, %{ref: ch_b, label: "chat", id: 2}})
    Process.sleep(20)

    send(session, {:xgw, :datachannel_closed, peer_b, ch_b})
    Process.sleep(20)

    send(fwd, {:xgw, :datachannel_message, peer_a, ch_a, "after-close"})
    Process.sleep(30)

    assert FakePeer.drain(peer_b) == []
    assert FakePeer.drain(peer_a) == []
  end

  test "sctp disconnected clears all labels for that peer" do
    assert {:ok, session} =
             Session.create("t1", "dc-sctp-clear", :datachannel, mode: :fanout_except_sender)

    fwd = Session.forwarder(session)
    assert {:ok, peer_a} = FakePeer.start_link()
    assert {:ok, peer_b} = FakePeer.start_link()
    ch_a = make_ref()
    ch_b = make_ref()

    a = %Participant{id: "a", peer_ref: peer_a, role: :publisher}
    b = %Participant{id: "b", peer_ref: peer_b, role: :publisher}

    :sys.replace_state(session, fn state ->
      {:ok, backend_state} = state.backend.participant_joined(a, state.backend_state)
      {:ok, backend_state} = state.backend.participant_joined(b, backend_state)

      %{
        state
        | participants: %{"a" => a, "b" => b},
          backend_state: backend_state
      }
    end)

    assert :ok = Forwarder.install_echo(fwd, a)
    assert :ok = Forwarder.install_echo(fwd, b)

    send(session, {:xgw, :datachannel, peer_a, %{ref: ch_a, label: "chat", id: 1}})
    send(session, {:xgw, :datachannel, peer_b, %{ref: ch_b, label: "chat", id: 2}})
    Process.sleep(20)

    send(session, {:xgw, :sctp_disconnected, peer_b})
    Process.sleep(20)

    send(fwd, {:xgw, :datachannel_message, peer_a, ch_a, "after-sctp"})
    Process.sleep(30)

    assert FakePeer.drain(peer_b) == []
  end

  test "on_no_channel buffer: flush reaches FakePeer when dest opens via Session" do
    assert {:ok, session} =
             Session.create("t1", "dc-buf", :datachannel,
               mode: :fanout_except_sender,
               on_no_channel: :buffer,
               labels: ["chat"],
               default_label: "chat"
             )

    fwd = Session.forwarder(session)
    assert :sys.get_state(fwd).backend_state.on_no_channel == :buffer

    assert {:ok, peer_a} = FakePeer.start_link()
    assert {:ok, peer_b} = FakePeer.start_link()
    ch_a = make_ref()
    ch_b = make_ref()

    a = %Participant{id: "a", peer_ref: peer_a, role: :publisher}
    b = %Participant{id: "b", peer_ref: peer_b, role: :publisher}

    :sys.replace_state(session, fn state ->
      {:ok, backend_state} = state.backend.participant_joined(a, state.backend_state)
      {:ok, backend_state} = state.backend.participant_joined(b, backend_state)

      %{
        state
        | participants: %{"a" => a, "b" => b},
          backend_state: backend_state
      }
    end)

    assert :ok = Forwarder.install_echo(fwd, a)
    assert :ok = Forwarder.install_echo(fwd, b)

    send(session, {:xgw, :datachannel, peer_a, %{ref: ch_a, label: "chat", id: 1}})
    Process.sleep(30)

    send(fwd, {:xgw, :datachannel_message, peer_a, ch_a, "buffered-hello"})
    Process.sleep(30)
    assert FakePeer.drain(peer_b) == []

    send(session, {:xgw, :datachannel, peer_b, %{ref: ch_b, label: "chat", id: 2}})

    assert wait_drain(peer_b, [{:send_datachannel, ch_b, "buffered-hello", :string}])
  end

  test "state_coalesce: stamps from and drops stale via Forwarder" do
    assert {:ok, session} =
             Session.create("t1", "dc-state", :datachannel,
               mode: :mfu,
               state_coalesce: true,
               labels: ["state"]
             )

    fwd = Session.forwarder(session)
    assert is_function(:sys.get_state(fwd).backend_state.types["state"])

    assert {:ok, peer_a} = FakePeer.start_link()
    assert {:ok, peer_b} = FakePeer.start_link()
    ch_a = make_ref()
    ch_b = make_ref()

    a = %Participant{id: "a", peer_ref: peer_a, role: :publisher}
    b = %Participant{id: "b", peer_ref: peer_b, role: :publisher}

    :sys.replace_state(session, fn state ->
      {:ok, backend_state} = state.backend.participant_joined(a, state.backend_state)
      {:ok, backend_state} = state.backend.participant_joined(b, backend_state)

      %{
        state
        | participants: %{"a" => a, "b" => b},
          backend_state: backend_state
      }
    end)

    assert :ok = Forwarder.install_echo(fwd, a)
    assert :ok = Forwarder.install_echo(fwd, b)

    send(session, {:xgw, :datachannel, peer_a, %{ref: ch_a, label: "state", id: 1}})
    send(session, {:xgw, :datachannel, peer_b, %{ref: ch_b, label: "state", id: 2}})
    Process.sleep(30)

    p2 = ~s({"type":"state","topic":"transform","seq":2,"body":{"x":2}})
    send(fwd, {:xgw, :datachannel_message, peer_a, ch_a, p2})

    assert wait_drain_match(peer_b, fn msgs ->
             state_delivered?(msgs, ch_b, "a", 2)
           end)

    assert FakePeer.drain(peer_a) == []

    stale = ~s({"type":"state","topic":"transform","seq":1,"body":{"x":1}})
    send(fwd, {:xgw, :datachannel_message, peer_a, ch_a, stale})
    Process.sleep(30)
    assert FakePeer.drain(peer_b) == []
    assert FakePeer.drain(peer_a) == []

    p3 = ~s({"type":"state","topic":"transform","seq":3,"body":{"x":3}})
    send(fwd, {:xgw, :datachannel_message, peer_a, ch_a, p3})

    assert wait_drain_match(peer_b, fn msgs ->
             state_delivered?(msgs, ch_b, "a", 3)
           end)
  end

  test "rpc_router: req reaches only to; res returns to caller via Forwarder" do
    assert {:ok, session} =
             Session.create("t1", "dc-rpc", :datachannel,
               mode: :mfu,
               rpc_router: true,
               labels: ["rpc"]
             )

    fwd = Session.forwarder(session)
    assert is_function(:sys.get_state(fwd).backend_state.types["rpc.req"])

    assert {:ok, peer_a} = FakePeer.start_link()
    assert {:ok, peer_b} = FakePeer.start_link()
    assert {:ok, peer_c} = FakePeer.start_link()
    ch_a = make_ref()
    ch_b = make_ref()
    ch_c = make_ref()

    a = %Participant{id: "a", peer_ref: peer_a, role: :publisher}
    b = %Participant{id: "b", peer_ref: peer_b, role: :publisher}
    c = %Participant{id: "c", peer_ref: peer_c, role: :publisher}

    :sys.replace_state(session, fn state ->
      {:ok, backend_state} = state.backend.participant_joined(a, state.backend_state)
      {:ok, backend_state} = state.backend.participant_joined(b, backend_state)
      {:ok, backend_state} = state.backend.participant_joined(c, backend_state)

      %{
        state
        | participants: %{"a" => a, "b" => b, "c" => c},
          backend_state: backend_state
      }
    end)

    assert :ok = Forwarder.install_echo(fwd, a)
    assert :ok = Forwarder.install_echo(fwd, b)
    assert :ok = Forwarder.install_echo(fwd, c)

    send(session, {:xgw, :datachannel, peer_a, %{ref: ch_a, label: "rpc", id: 1}})
    send(session, {:xgw, :datachannel, peer_b, %{ref: ch_b, label: "rpc", id: 2}})
    send(session, {:xgw, :datachannel, peer_c, %{ref: ch_c, label: "rpc", id: 3}})
    Process.sleep(30)

    req = ~s({"type":"rpc.req","id":"r1","method":"ping","to":"b","payload":{}})
    send(fwd, {:xgw, :datachannel_message, peer_a, ch_a, req})

    assert wait_drain_match(peer_b, fn msgs ->
             match?([{:send_datachannel, ^ch_b, _, :string}], msgs)
           end)

    assert FakePeer.drain(peer_c) == []

    res = ~s({"type":"rpc.res","id":"r1","payload":{"ok":true}})
    send(fwd, {:xgw, :datachannel_message, peer_b, ch_b, res})

    assert wait_drain_match(peer_a, fn msgs ->
             match?([{:send_datachannel, ^ch_a, _, :string}], msgs)
           end)
  end

  test "Msgpack Session Forwarder fanout uses binary PPI on Peer cast" do
    alias Xylon.Datachannel.Codec.Msgpack

    assert {:ok, session} =
             Session.create("t1", "dc-mp-ppi", :datachannel,
               mode: :mfu,
               codec: Msgpack,
               types: %{"chat" => :passthrough},
               labels: ["chat"],
               default_label: "chat"
             )

    fwd = Session.forwarder(session)
    assert {:ok, peer_a} = FakePeer.start_link()
    assert {:ok, peer_b} = FakePeer.start_link()
    ch_a = make_ref()
    ch_b = make_ref()

    a = %Participant{id: "a", peer_ref: peer_a, role: :publisher}
    b = %Participant{id: "b", peer_ref: peer_b, role: :publisher}

    :sys.replace_state(session, fn state ->
      assert state.backend_state.dc_ppi == :binary
      {:ok, backend_state} = state.backend.participant_joined(a, state.backend_state)
      {:ok, backend_state} = state.backend.participant_joined(b, backend_state)

      %{
        state
        | participants: %{"a" => a, "b" => b},
          backend_state: backend_state
      }
    end)

    assert :ok = Forwarder.install_echo(fwd, a)
    assert :ok = Forwarder.install_echo(fwd, b)

    send(session, {:xgw, :datachannel, peer_a, %{ref: ch_a, label: "chat", id: 1}})
    send(session, {:xgw, :datachannel, peer_b, %{ref: ch_b, label: "chat", id: 2}})
    Process.sleep(30)

    _ = XGW.Peer.SendHandle.invalidate(peer_b)

    assert {:ok, payload} = Msgpack.encode(%{"type" => "chat", "text" => "bin-ppi"})
    send(fwd, {:xgw, :datachannel_message, peer_a, ch_a, payload})

    assert wait_drain_match(peer_b, fn msgs ->
             case msgs do
               [{:send_datachannel, ^ch_b, out, :binary}] when is_binary(out) -> true
               _ -> false
             end
           end)

    assert FakePeer.drain_modes(peer_b) == [:cast]
  end

  test "opaque chat fanout reaches trunk-like Participant with label isolation" do
    assert {:ok, session} =
             Session.create("t1", "dc-cascade", :datachannel, mode: :fanout_except_sender)

    fwd = Session.forwarder(session)
    assert {:ok, peer_a} = FakePeer.start_link()
    assert {:ok, peer_b} = FakePeer.start_link()
    assert {:ok, peer_t} = FakePeer.start_link()
    ch_a = make_ref()
    ch_b = make_ref()
    ch_t_chat = make_ref()
    ch_t_rpc = make_ref()

    a = %Participant{id: "a", peer_ref: peer_a, role: :publisher}
    b = %Participant{id: "b", peer_ref: peer_b, role: :publisher}
    trunk = %Participant{id: "trunk1", peer_ref: peer_t, role: :trunk}

    :sys.replace_state(session, fn state ->
      {:ok, backend_state} = state.backend.participant_joined(a, state.backend_state)
      {:ok, backend_state} = state.backend.participant_joined(b, backend_state)
      {:ok, backend_state} = state.backend.participant_joined(trunk, backend_state)

      %{
        state
        | participants: %{"a" => a, "b" => b, "trunk1" => trunk},
          backend_state: backend_state
      }
    end)

    assert :ok = Forwarder.install_echo(fwd, a)
    assert :ok = Forwarder.install_echo(fwd, b)
    assert :ok = Forwarder.install_echo(fwd, trunk)

    send(session, {:xgw, :datachannel, peer_a, %{ref: ch_a, label: "chat", id: 1}})
    send(session, {:xgw, :datachannel, peer_b, %{ref: ch_b, label: "chat", id: 2}})
    send(session, {:xgw, :datachannel, peer_t, %{ref: ch_t_chat, label: "chat", id: 3}})
    send(session, {:xgw, :datachannel, peer_t, %{ref: ch_t_rpc, label: "rpc", id: 4}})
    Process.sleep(30)

    _ = XGW.Peer.SendHandle.invalidate(peer_b)
    _ = XGW.Peer.SendHandle.invalidate(peer_t)

    payload = "cascade-chat"
    send(fwd, {:xgw, :datachannel_message, peer_a, ch_a, payload})

    assert wait_drain(peer_b, [{:send_datachannel, ch_b, payload, :string}])
    assert wait_drain(peer_t, [{:send_datachannel, ch_t_chat, payload, :string}])
    assert FakePeer.drain(peer_a) == []
    # Label isolation: trunk rpc channel must not receive chat bytes.
    assert FakePeer.drain(peer_t) == []
  end

  test "fanout_subset with trunk includes trunk and excludes outsider" do
    assert {:ok, session} =
             Session.create("t1", "dc-subset-trunk", :datachannel,
               mode: :fanout_subset,
               subset: ["trunk1"]
             )

    fwd = Session.forwarder(session)
    assert {:ok, peer_a} = FakePeer.start_link()
    assert {:ok, peer_b} = FakePeer.start_link()
    assert {:ok, peer_t} = FakePeer.start_link()
    ch_a = make_ref()
    ch_b = make_ref()
    ch_t = make_ref()

    a = %Participant{id: "a", peer_ref: peer_a, role: :publisher}
    b = %Participant{id: "b", peer_ref: peer_b, role: :publisher}
    trunk = %Participant{id: "trunk1", peer_ref: peer_t, role: :trunk}

    :sys.replace_state(session, fn state ->
      {:ok, backend_state} = state.backend.participant_joined(a, state.backend_state)
      {:ok, backend_state} = state.backend.participant_joined(b, backend_state)
      {:ok, backend_state} = state.backend.participant_joined(trunk, backend_state)

      %{
        state
        | participants: %{"a" => a, "b" => b, "trunk1" => trunk},
          backend_state: backend_state
      }
    end)

    assert :ok = Forwarder.install_echo(fwd, a)
    assert :ok = Forwarder.install_echo(fwd, b)
    assert :ok = Forwarder.install_echo(fwd, trunk)

    send(session, {:xgw, :datachannel, peer_a, %{ref: ch_a, label: "chat", id: 1}})
    send(session, {:xgw, :datachannel, peer_b, %{ref: ch_b, label: "chat", id: 2}})
    send(session, {:xgw, :datachannel, peer_t, %{ref: ch_t, label: "chat", id: 3}})
    Process.sleep(30)

    _ = XGW.Peer.SendHandle.invalidate(peer_b)
    _ = XGW.Peer.SendHandle.invalidate(peer_t)

    payload = "subset-trunk"
    send(fwd, {:xgw, :datachannel_message, peer_a, ch_a, payload})

    assert wait_drain(peer_t, [{:send_datachannel, ch_t, payload, :string}])
    Process.sleep(40)
    assert FakePeer.drain(peer_b) == []
    assert FakePeer.drain(peer_a) == []
  end

  test "trunk-role sender ignores client to and uses Session default" do
    assert {:ok, session} =
             Session.create("t1", "dc-trunk-opaque-to", :datachannel, mode: :fanout_except_sender)

    fwd = Session.forwarder(session)
    assert {:ok, peer_a} = FakePeer.start_link()
    assert {:ok, peer_t} = FakePeer.start_link()
    ch_a = make_ref()
    ch_t = make_ref()

    a = %Participant{id: "a", peer_ref: peer_a, role: :publisher}
    trunk = %Participant{id: "trunk1", peer_ref: peer_t, role: :trunk}

    :sys.replace_state(session, fn state ->
      {:ok, backend_state} = state.backend.participant_joined(a, state.backend_state)
      {:ok, backend_state} = state.backend.participant_joined(trunk, backend_state)

      %{
        state
        | participants: %{"a" => a, "trunk1" => trunk},
          backend_state: backend_state
      }
    end)

    assert :ok = Forwarder.install_echo(fwd, a)
    assert :ok = Forwarder.install_echo(fwd, trunk)

    send(session, {:xgw, :datachannel, peer_a, %{ref: ch_a, label: "chat", id: 1}})
    send(session, {:xgw, :datachannel, peer_t, %{ref: ch_t, label: "chat", id: 2}})
    Process.sleep(30)

    _ = XGW.Peer.SendHandle.invalidate(peer_a)

    # Embedded to would only target the trunk id; hop must stay opaque.
    payload = ~s({"type":"chat","text":"hop","to":["trunk1"]})
    send(fwd, {:xgw, :datachannel_message, peer_t, ch_t, payload})

    assert wait_drain(peer_a, [{:send_datachannel, ch_a, payload, :string}])
  end

  test "mfu chat passthrough reaches trunk unchanged" do
    assert {:ok, session} =
             Session.create("t1", "dc-cascade-mfu", :datachannel,
               mode: :mfu,
               types: %{"chat" => :passthrough},
               labels: ["chat"],
               default_label: "chat"
             )

    fwd = Session.forwarder(session)
    assert {:ok, peer_a} = FakePeer.start_link()
    assert {:ok, peer_t} = FakePeer.start_link()
    ch_a = make_ref()
    ch_t = make_ref()

    a = %Participant{id: "a", peer_ref: peer_a, role: :publisher}
    trunk = %Participant{id: "trunk1", peer_ref: peer_t, role: :trunk}

    :sys.replace_state(session, fn state ->
      {:ok, backend_state} = state.backend.participant_joined(a, state.backend_state)
      {:ok, backend_state} = state.backend.participant_joined(trunk, backend_state)

      %{
        state
        | participants: %{"a" => a, "trunk1" => trunk},
          backend_state: backend_state
      }
    end)

    assert :ok = Forwarder.install_echo(fwd, a)
    assert :ok = Forwarder.install_echo(fwd, trunk)

    send(session, {:xgw, :datachannel, peer_a, %{ref: ch_a, label: "chat", id: 1}})
    send(session, {:xgw, :datachannel, peer_t, %{ref: ch_t, label: "chat", id: 2}})
    Process.sleep(30)

    _ = XGW.Peer.SendHandle.invalidate(peer_t)

    payload = ~s({"type":"chat","text":"hop"})
    send(fwd, {:xgw, :datachannel_message, peer_a, ch_a, payload})

    assert wait_drain(peer_t, [{:send_datachannel, ch_t, payload, :string}])
    assert FakePeer.drain(peer_a) == []
  end

  test "client to id list fans out only to listed peers" do
    assert {:ok, session} =
             Session.create("t1", "dc-client-dest", :datachannel, mode: :fanout_except_sender)

    fwd = Session.forwarder(session)
    assert {:ok, peer_a} = FakePeer.start_link()
    assert {:ok, peer_b} = FakePeer.start_link()
    assert {:ok, peer_c} = FakePeer.start_link()
    assert {:ok, peer_d} = FakePeer.start_link()
    ch_a = make_ref()
    ch_b = make_ref()
    ch_c = make_ref()
    ch_d = make_ref()

    a = %Participant{id: "a", peer_ref: peer_a, role: :publisher}
    b = %Participant{id: "b", peer_ref: peer_b, role: :publisher}
    c = %Participant{id: "c", peer_ref: peer_c, role: :publisher}
    d = %Participant{id: "d", peer_ref: peer_d, role: :publisher}

    :sys.replace_state(session, fn state ->
      {:ok, backend_state} = state.backend.participant_joined(a, state.backend_state)
      {:ok, backend_state} = state.backend.participant_joined(b, backend_state)
      {:ok, backend_state} = state.backend.participant_joined(c, backend_state)
      {:ok, backend_state} = state.backend.participant_joined(d, backend_state)

      %{
        state
        | participants: %{"a" => a, "b" => b, "c" => c, "d" => d},
          backend_state: backend_state
      }
    end)

    for {p, peer, ch, id} <- [
          {a, peer_a, ch_a, 1},
          {b, peer_b, ch_b, 2},
          {c, peer_c, ch_c, 3},
          {d, peer_d, ch_d, 4}
        ] do
      assert :ok = Forwarder.install_echo(fwd, p)
      send(session, {:xgw, :datachannel, peer, %{ref: ch, label: "chat", id: id}})
    end

    Process.sleep(30)

    payload = ~s({"type":"chat","text":"hi","to":["b","c"]})
    send(fwd, {:xgw, :datachannel_message, peer_a, ch_a, payload})

    assert wait_drain(peer_b, [{:send_datachannel, ch_b, payload, :string}])
    assert wait_drain(peer_c, [{:send_datachannel, ch_c, payload, :string}])
    assert FakePeer.drain(peer_d) == []
    assert FakePeer.drain(peer_a) == []
  end

  test "egress route replaces Session default; client to still wins" do
    assert {:ok, session} =
             Session.create("t1", "dc-egress", :datachannel, mode: :fanout_except_sender)

    fwd = Session.forwarder(session)
    assert {:ok, peer_a} = FakePeer.start_link()
    assert {:ok, peer_b} = FakePeer.start_link()
    assert {:ok, peer_c} = FakePeer.start_link()
    ch_a = make_ref()
    ch_b = make_ref()
    ch_c = make_ref()

    a = %Participant{id: "a", peer_ref: peer_a, role: :publisher}
    b = %Participant{id: "b", peer_ref: peer_b, role: :publisher}
    c = %Participant{id: "c", peer_ref: peer_c, role: :publisher}

    :sys.replace_state(session, fn state ->
      {:ok, backend_state} = state.backend.participant_joined(a, state.backend_state)
      {:ok, backend_state} = state.backend.participant_joined(b, backend_state)
      {:ok, backend_state} = state.backend.participant_joined(c, backend_state)

      %{
        state
        | participants: %{"a" => a, "b" => b, "c" => c},
          backend_state: backend_state
      }
    end)

    for {p, peer, ch, id} <- [{a, peer_a, ch_a, 1}, {b, peer_b, ch_b, 2}, {c, peer_c, ch_c, 3}] do
      assert :ok = Forwarder.install_echo(fwd, p)
      send(session, {:xgw, :datachannel, peer, %{ref: ch, label: "chat", id: id}})
    end

    Process.sleep(30)

    assert :ok = Xylon.Datachannel.set_route(session, "a", ["b"])

    payload = ~s({"type":"chat","text":"routed"})
    send(fwd, {:xgw, :datachannel_message, peer_a, ch_a, payload})
    assert wait_drain(peer_b, [{:send_datachannel, ch_b, payload, :string}])
    assert FakePeer.drain(peer_c) == []
    assert FakePeer.drain(peer_a) == []

    targeted = ~s({"type":"chat","text":"to","to":"c"})
    send(fwd, {:xgw, :datachannel_message, peer_a, ch_a, targeted})
    assert wait_drain(peer_c, [{:send_datachannel, ch_c, targeted, :string}])
    assert FakePeer.drain(peer_b) == []

    assert :ok = Xylon.Datachannel.set_route(session, "a", [])
    send(fwd, {:xgw, :datachannel_message, peer_a, ch_a, payload})
    Process.sleep(40)
    assert FakePeer.drain(peer_b) == []
    assert FakePeer.drain(peer_c) == []

    assert :ok = Xylon.Datachannel.clear_route(session, "a")
    send(fwd, {:xgw, :datachannel_message, peer_a, ch_a, payload})
    assert wait_drain(peer_b, [{:send_datachannel, ch_b, payload, :string}])
    assert wait_drain(peer_c, [{:send_datachannel, ch_c, payload, :string}])
  end

  test "leave clears egress routes for sender" do
    assert {:ok, session} =
             Session.create("t1", "dc-egress-leave", :datachannel, mode: :fanout_except_sender)

    assert {:ok, peer_a} = FakePeer.start_link()
    assert {:ok, peer_b} = FakePeer.start_link()
    a = %Participant{id: "a", peer_ref: peer_a, role: :publisher}
    b = %Participant{id: "b", peer_ref: peer_b, role: :publisher}

    :sys.replace_state(session, fn state ->
      {:ok, backend_state} = state.backend.participant_joined(a, state.backend_state)
      {:ok, backend_state} = state.backend.participant_joined(b, backend_state)
      %{state | participants: %{"a" => a, "b" => b}, backend_state: backend_state}
    end)

    assert :ok = Xylon.Datachannel.set_route(session, "a", ["b"])
    assert :ok = Xylon.Datachannel.set_route(session, "b", ["a"])
    plan = Session.plan_ref(session)
    assert {:ok, ["b"]} = Xylon.Datachannel.EgressRoute.get(plan, "a", "chat")
    assert {:ok, ["a"]} = Xylon.Datachannel.EgressRoute.get(plan, "b", "chat")

    :sys.replace_state(session, fn state ->
      {:ok, backend_state} = state.backend.participant_left(a, state.backend_state)
      %{state | participants: %{"b" => b}, backend_state: backend_state}
    end)

    assert :default = Xylon.Datachannel.EgressRoute.get(plan, "a", "chat")
    assert {:ok, []} = Xylon.Datachannel.EgressRoute.get(plan, "b", "chat")

    assert {:error, :unknown_sender} = Xylon.Datachannel.set_route(session, "ghost", ["b"])

    assert :ok = Xylon.Datachannel.set_route(session, "b", ["x"], label: "chat")
    assert :ok = Xylon.Datachannel.set_route(session, "b", :except_sender)
    assert :ok = Xylon.Datachannel.clear_route(session, "b")
    assert :default = Xylon.Datachannel.EgressRoute.get(plan, "b", "chat")
    assert :default = Xylon.Datachannel.EgressRoute.get(plan, "b", :_)
  end

  test "client to filter map is rejected" do
    assert {:ok, session} =
             Session.create("t1", "dc-bad-dest", :datachannel, mode: :fanout_except_sender)

    fwd = Session.forwarder(session)
    assert {:ok, peer_a} = FakePeer.start_link()
    assert {:ok, peer_b} = FakePeer.start_link()
    ch_a = make_ref()
    ch_b = make_ref()

    a = %Participant{id: "a", peer_ref: peer_a, role: :publisher}
    b = %Participant{id: "b", peer_ref: peer_b, role: :publisher}

    :sys.replace_state(session, fn state ->
      {:ok, backend_state} = state.backend.participant_joined(a, state.backend_state)
      {:ok, backend_state} = state.backend.participant_joined(b, backend_state)

      %{
        state
        | participants: %{"a" => a, "b" => b},
          backend_state: backend_state
      }
    end)

    assert :ok = Forwarder.install_echo(fwd, a)
    assert :ok = Forwarder.install_echo(fwd, b)
    send(session, {:xgw, :datachannel, peer_a, %{ref: ch_a, label: "chat", id: 1}})
    send(session, {:xgw, :datachannel, peer_b, %{ref: ch_b, label: "chat", id: 2}})
    Process.sleep(20)

    payload = ~s({"type":"chat","text":"hi","to":{"role":"mod"}})
    send(fwd, {:xgw, :datachannel_message, peer_a, ch_a, payload})
    Process.sleep(50)
    assert FakePeer.drain(peer_b) == []
    assert FakePeer.drain(peer_a) == []
  end

  test "authorize can deny client dest send" do
    deny = fn meta -> meta[:dir] != :send end

    assert {:ok, session} =
             Session.create("t1", "dc-auth-dest", :datachannel,
               mode: :fanout_except_sender,
               authorize: deny
             )

    fwd = Session.forwarder(session)
    assert {:ok, peer_a} = FakePeer.start_link()
    assert {:ok, peer_b} = FakePeer.start_link()
    ch_a = make_ref()
    ch_b = make_ref()

    a = %Participant{id: "a", peer_ref: peer_a, role: :publisher}
    b = %Participant{id: "b", peer_ref: peer_b, role: :publisher}

    :sys.replace_state(session, fn state ->
      {:ok, backend_state} = state.backend.participant_joined(a, state.backend_state)
      {:ok, backend_state} = state.backend.participant_joined(b, backend_state)

      %{
        state
        | participants: %{"a" => a, "b" => b},
          backend_state: backend_state
      }
    end)

    assert :ok = Forwarder.install_echo(fwd, a)
    assert :ok = Forwarder.install_echo(fwd, b)
    send(session, {:xgw, :datachannel, peer_a, %{ref: ch_a, label: "chat", id: 1}})
    send(session, {:xgw, :datachannel, peer_b, %{ref: ch_b, label: "chat", id: 2}})
    Process.sleep(20)

    payload = ~s({"type":"chat","text":"hi","to":"b"})
    send(fwd, {:xgw, :datachannel_message, peer_a, ch_a, payload})
    Process.sleep(50)
    assert FakePeer.drain(peer_b) == []
  end

  test "msgpack client to id list fans out only to listed peers" do
    alias Xylon.Datachannel.Codec.Msgpack

    assert {:ok, session} =
             Session.create("t1", "dc-mp-dest", :datachannel,
               mode: :mfu,
               codec: Msgpack,
               types: %{"chat" => :passthrough},
               labels: ["chat"],
               default_label: "chat"
             )

    fwd = Session.forwarder(session)
    assert {:ok, peer_a} = FakePeer.start_link()
    assert {:ok, peer_b} = FakePeer.start_link()
    assert {:ok, peer_c} = FakePeer.start_link()
    assert {:ok, peer_d} = FakePeer.start_link()
    ch_a = make_ref()
    ch_b = make_ref()
    ch_c = make_ref()
    ch_d = make_ref()

    a = %Participant{id: "a", peer_ref: peer_a, role: :publisher}
    b = %Participant{id: "b", peer_ref: peer_b, role: :publisher}
    c = %Participant{id: "c", peer_ref: peer_c, role: :publisher}
    d = %Participant{id: "d", peer_ref: peer_d, role: :publisher}

    :sys.replace_state(session, fn state ->
      {:ok, backend_state} = state.backend.participant_joined(a, state.backend_state)
      {:ok, backend_state} = state.backend.participant_joined(b, backend_state)
      {:ok, backend_state} = state.backend.participant_joined(c, backend_state)
      {:ok, backend_state} = state.backend.participant_joined(d, backend_state)

      %{
        state
        | participants: %{"a" => a, "b" => b, "c" => c, "d" => d},
          backend_state: backend_state
      }
    end)

    for {p, peer, ch, id} <- [
          {a, peer_a, ch_a, 1},
          {b, peer_b, ch_b, 2},
          {c, peer_c, ch_c, 3},
          {d, peer_d, ch_d, 4}
        ] do
      assert :ok = Forwarder.install_echo(fwd, p)
      send(session, {:xgw, :datachannel, peer, %{ref: ch, label: "chat", id: id}})
    end

    Process.sleep(30)
    _ = XGW.Peer.SendHandle.invalidate(peer_b)
    _ = XGW.Peer.SendHandle.invalidate(peer_c)
    _ = XGW.Peer.SendHandle.invalidate(peer_d)

    assert {:ok, payload} =
             Msgpack.encode(%{"type" => "chat", "text" => "hi", "to" => ["b", "c"]})

    send(fwd, {:xgw, :datachannel_message, peer_a, ch_a, payload})

    assert wait_drain_match(peer_b, fn msgs ->
             match?([{:send_datachannel, ^ch_b, ^payload, :binary}], msgs)
           end)

    assert wait_drain_match(peer_c, fn msgs ->
             match?([{:send_datachannel, ^ch_c, ^payload, :binary}], msgs)
           end)

    assert FakePeer.drain(peer_d) == []
    assert FakePeer.drain(peer_a) == []
  end

  defp state_delivered?(msgs, ch, from, seq) do
    case msgs do
      [{:send_datachannel, ^ch, bin, _ppi}] when is_binary(bin) ->
        case Jason.decode(bin) do
          {:ok, %{"from" => ^from, "seq" => ^seq, "type" => "state"}} -> true
          _ -> false
        end

      _ ->
        false
    end
  end

  defp wait_drain(peer, expected) do
    Enum.reduce_while(1..50, false, fn _, _ ->
      case FakePeer.drain(peer) do
        ^expected ->
          {:halt, true}

        [] ->
          Process.sleep(10)
          {:cont, false}

        other ->
          flunk("unexpected: #{inspect(other)}")
      end
    end)
  end

  defp wait_drain_match(peer, pred) when is_function(pred, 1) do
    Enum.reduce_while(1..50, false, fn _, _ ->
      case FakePeer.drain(peer) do
        [] ->
          Process.sleep(10)
          {:cont, false}

        msgs ->
          if pred.(msgs), do: {:halt, true}, else: flunk("unexpected: #{inspect(msgs)}")
      end
    end)
  end
end
