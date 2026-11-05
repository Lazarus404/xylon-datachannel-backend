defmodule Xylon.Datachannel.TrunkCascadeTest do
  use ExUnit.Case, async: false

  alias XGW.Trunk.Pipe
  alias Xylon.Datachannel
  alias Xylon.Trunk.{Bridge, Manager}
  alias Xylon.{Forwarder, Participant, Session}

  defmodule FakePeer do
    @moduledoc false
    use GenServer

    def start_link, do: GenServer.start_link(__MODULE__, [])
    def drain(pid), do: GenServer.call(pid, :drain)

    @impl true
    def init(_), do: {:ok, []}

    @impl true
    def handle_call({:send_datachannel, ref, data, type}, _from, state) do
      {:reply, :ok, [{:send_datachannel, ref, data, type} | state]}
    end

    def handle_call(:drain, _from, state), do: {:reply, Enum.reverse(state), []}

    @impl true
    def handle_cast({:send_datachannel, ref, data, type}, state) do
      {:noreply, [{:send_datachannel, ref, data, type} | state]}
    end

    def handle_cast({:send_datachannel, ref, data}, state) do
      handle_cast({:send_datachannel, ref, data, :string}, state)
    end
  end

  @master_key <<0xE1F97A0D3E018BE0D64FA32C06DE4139::size(128)>>
  @master_salt <<0x0EC675AD498AFEEBB6960B3AABE6::size(112)>>

  setup do
    Xylon.Catalogue.reset!()
    assert :ok = Datachannel.attach()
    :ok
  end

  test "opaque chat crosses UDP trunk with label isolation" do
    assert {:ok, session_a} =
             Session.create("t1", "dc-edge-a", :datachannel, mode: :fanout_except_sender)

    assert {:ok, session_b} =
             Session.create("t1", "dc-edge-b", :datachannel, mode: :fanout_except_sender)

    fwd_a = Session.forwarder(session_a)
    fwd_b = Session.forwarder(session_b)

    assert {:ok, %{a: trunk_a, b: trunk_b}} =
             Manager.connect_pair(session_a, session_b,
               id_a: "trunk_ab",
               id_b: "trunk_ba",
               master_key: @master_key,
               master_salt: @master_salt
             )

    assert {:ok, bridge_a} = Bridge.start_link(forwarder: fwd_a)
    assert {:ok, bridge_b} = Bridge.start_link(forwarder: fwd_b)
    assert :ok = Bridge.attach_pipe(bridge_a, trunk_a.peer_ref)
    assert :ok = Bridge.attach_pipe(bridge_b, trunk_b.peer_ref)

    assert {:ok, peer_a} = FakePeer.start_link()
    assert {:ok, peer_b} = FakePeer.start_link()

    a = %Participant{id: "a", peer_ref: peer_a, role: :publisher}
    b = %Participant{id: "b", peer_ref: peer_b, role: :publisher}

    :sys.replace_state(session_a, fn state ->
      {:ok, backend_state} = state.backend.participant_joined(a, state.backend_state)
      {:ok, backend_state} = state.backend.participant_joined(trunk_a, backend_state)

      %{
        state
        | participants: %{"a" => a, "trunk_ab" => trunk_a},
          backend_state: backend_state
      }
    end)

    :sys.replace_state(session_b, fn state ->
      {:ok, backend_state} = state.backend.participant_joined(b, state.backend_state)
      {:ok, backend_state} = state.backend.participant_joined(trunk_b, backend_state)

      %{
        state
        | participants: %{"b" => b, "trunk_ba" => trunk_b},
          backend_state: backend_state
      }
    end)

    assert :ok = Forwarder.install_echo(fwd_a, a)
    assert :ok = Forwarder.install_echo(fwd_a, trunk_a)
    assert :ok = Forwarder.install_echo(fwd_b, b)
    assert :ok = Forwarder.install_echo(fwd_b, trunk_b)

    ch_a = make_ref()
    ch_b = make_ref()
    assert {:ok, ch_ta} = Pipe.ensure_label(trunk_a.peer_ref, "chat")
    assert {:ok, ch_tb} = Pipe.ensure_label(trunk_b.peer_ref, "chat")
    assert {:ok, ch_ta_rpc} = Pipe.ensure_label(trunk_a.peer_ref, "rpc")

    send(session_a, {:xgw, :datachannel, peer_a, %{ref: ch_a, label: "chat", id: 1}})
    send(session_a, {:xgw, :datachannel, trunk_a.peer_ref, %{ref: ch_ta, label: "chat", id: 2}})
    send(session_a, {:xgw, :datachannel, trunk_a.peer_ref, %{ref: ch_ta_rpc, label: "rpc", id: 3}})
    send(session_b, {:xgw, :datachannel, peer_b, %{ref: ch_b, label: "chat", id: 4}})
    send(session_b, {:xgw, :datachannel, trunk_b.peer_ref, %{ref: ch_tb, label: "chat", id: 5}})
    Process.sleep(40)

    before = Pipe.stats(trunk_a.peer_ref)
    payload = "cross-edge-chat"
    send(fwd_a, {:xgw, :datachannel_message, peer_a, ch_a, payload})

    assert wait_drain(peer_b, [{:send_datachannel, ch_b, payload, :string}])
    after_stats = Pipe.stats(trunk_a.peer_ref)
    assert after_stats.udp_out > before.udp_out

    # rpc label on trunk A must not have received chat
    assert FakePeer.drain(peer_a) == []
  end

  defp wait_drain(peer, expected) do
    Enum.reduce_while(1..80, false, fn _, _ ->
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
end
