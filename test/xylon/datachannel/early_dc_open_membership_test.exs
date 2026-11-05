defmodule Xylon.Datachannel.EarlyDcOpenMembershipTest do
  use ExUnit.Case, async: false

  alias Xylon.Datachannel
  alias Xylon.Datachannel.Plan
  alias Xylon.{Participant, Session}

  defmodule FakePeer do
    @moduledoc false
    use GenServer

    def start_link, do: GenServer.start_link(__MODULE__, [])
    def drain(pid), do: GenServer.call(pid, :drain)

    @impl true
    def init(_), do: {:ok, %{events: []}}

    @impl true
    def handle_call({:send_datachannel, ref, data, type}, _from, state) do
      {:reply, :ok, %{state | events: [{:send_datachannel, ref, data, type} | state.events]}}
    end

    def handle_call(:drain, _from, state) do
      {:reply, Enum.reverse(state.events), %{state | events: []}}
    end

    @impl true
    def handle_cast({:send_datachannel, ref, data, type}, state) do
      {:noreply, %{state | events: [{:send_datachannel, ref, data, type} | state.events]}}
    end
  end

  setup do
    Xylon.Catalogue.reset!()
    assert :ok = Datachannel.attach()
    :ok
  end

  test "early Forwarder DC open before put_plan still ends with full membership after Session open" do
    assert {:ok, session} =
             Session.create("t1", "early-dc-#{System.unique_integer([:positive])}", :datachannel,
               mode: :fanout_except_sender
             )

    fwd = Session.forwarder(session)
    plan_ref = Session.plan_ref(session)

    Enum.each(0..5, fn i ->
      assert {:ok, peer} = FakePeer.start_link()
      ch = make_ref()
      # Race: open reaches Forwarder before install_forward_plan.
      send(fwd, {:xgw, :datachannel, peer, %{ref: ch, label: "chat", id: 0}})
      Process.sleep(1)

      p = %Participant{id: "p#{i}", peer_ref: peer, role: :publisher}
      assert :ok = Session.attach_participant(session, p)
      send(session, {:xgw, :datachannel, peer, %{ref: ch, label: "chat", id: 0}})
      Process.sleep(1)
    end)

    Process.sleep(50)

    members = plan_ref |> Plan.participant_ids() |> Enum.sort()
    assert members == ["p0", "p1", "p2", "p3", "p4", "p5"]

    Enum.each(members, fn id ->
      assert Plan.channel_ref(plan_ref, id, "chat")
      assert %Participant{} = Plan.get_participant(plan_ref, id)
    end)
  end

  test "fanout reaches all peers when Session registered channels after Forwarder miss" do
    assert {:ok, session} =
             Session.create("t1", "early-fan-#{System.unique_integer([:positive])}", :datachannel,
               mode: :fanout_except_sender
             )

    fwd = Session.forwarder(session)

    peers =
      Enum.map(0..5, fn i ->
        assert {:ok, peer} = FakePeer.start_link()
        {i, peer}
      end)

    channels = Map.new(peers, fn {i, _peer} -> {i, make_ref()} end)

    Enum.each(peers, fn {i, peer} ->
      ch = Map.fetch!(channels, i)
      send(fwd, {:xgw, :datachannel, peer, %{ref: ch, label: "chat", id: 0}})
      p = %Participant{id: "p#{i}", peer_ref: peer, role: :publisher}
      assert :ok = Session.attach_participant(session, p)
      send(session, {:xgw, :datachannel, peer, %{ref: ch, label: "chat", id: 0}})
    end)

    Process.sleep(30)

    {_i0, peer0} = hd(peers)
    ch0 = Map.fetch!(channels, 0)
    send(fwd, {:xgw, :datachannel_message, peer0, ch0, ~s({"type":"chat","text":"hi"})})

    Enum.each(1..5, fn i ->
      {_i, peer} = Enum.at(peers, i)
      ch = Map.fetch!(channels, i)

      assert wait_drain(peer, [
               {:send_datachannel, ch, ~s({"type":"chat","text":"hi"}), :string}
             ])
    end)
  end

  test "Plan ETS survives unrelated Session kill so other rooms keep members" do
    # setup already called Datachannel.attach -> Plan.init from a long-lived owner.
    assert :ets.whereis(:xylon_datachannel_plan) != :undefined

    assert {:ok, session_a} =
             Session.create("t1", "ets-a-#{System.unique_integer([:positive])}", :datachannel)

    assert {:ok, session_b} =
             Session.create("t1", "ets-b-#{System.unique_integer([:positive])}", :datachannel)

    plan_b = Session.plan_ref(session_b)
    assert {:ok, peer} = FakePeer.start_link()
    p = %Participant{id: "keep", peer_ref: peer, role: :publisher}
    assert :ok = Session.attach_participant(session_b, p)
    assert "keep" in Plan.participant_ids(plan_b)

    Process.exit(session_a, :kill)
    Process.sleep(50)

    assert :ets.whereis(:xylon_datachannel_plan) != :undefined
    assert "keep" in Plan.participant_ids(plan_b)
  end

  test "Forwarder buffers DC open until put_plan then registers channel" do
    assert {:ok, session} =
             Session.create("t1", "fwd-buf-#{System.unique_integer([:positive])}", :datachannel,
               mode: :fanout_except_sender
             )

    fwd = Session.forwarder(session)
    plan_ref = Session.plan_ref(session)
    assert {:ok, peer} = FakePeer.start_link()
    ch = make_ref()

    send(fwd, {:xgw, :datachannel, peer, %{ref: ch, label: "chat", id: 0}})
    Process.sleep(10)
    assert Plan.channel_ref(plan_ref, "p0", "chat") == nil

    p = %Participant{id: "p0", peer_ref: peer, role: :publisher}
    assert :ok = Session.attach_participant(session, p)
    Process.sleep(20)

    assert Plan.channel_ref(plan_ref, "p0", "chat") == ch
  end

  defp wait_drain(peer, expected, tries \\ 50)

  defp wait_drain(peer, expected, 0) do
    flunk("timeout waiting #{inspect(expected)} got #{inspect(FakePeer.drain(peer))}")
  end

  defp wait_drain(peer, expected, tries) do
    case FakePeer.drain(peer) do
      ^expected ->
        true

      [] ->
        Process.sleep(10)
        wait_drain(peer, expected, tries - 1)

      other ->
        flunk("unexpected drain #{inspect(other)} expected #{inspect(expected)}")
    end
  end
end
