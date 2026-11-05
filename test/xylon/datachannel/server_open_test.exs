defmodule Xylon.Datachannel.ServerOpenTest do
  use ExUnit.Case, async: false

  alias Xylon.Datachannel.Backend
  alias Xylon.{Participant, Session}

  defmodule StubPeer do
    @moduledoc false
    use GenServer

    def start_link(parent), do: GenServer.start_link(__MODULE__, parent)

    @impl true
    def init(parent), do: {:ok, %{parent: parent, opens: []}}

    @impl true
    def handle_call({:open_datachannel, label, opts}, _from, state) do
      info = %{ref: make_ref(), label: label, id: length(state.opens)}
      send(state.parent, {:opened, label, opts, info})
      {:reply, {:ok, info}, %{state | opens: [info | state.opens]}}
    end
  end

  test "maybe_server_open opens pending labels via Peer" do
    assert {:ok, peer} = StubPeer.start_link(self())

    assert {:ok, state} =
             Backend.init(%Session{id: "s1", tenant_id: "t1"},
               mode: :fanout_except_sender,
               labels: ["chat", "rpc", "state"],
               server_open_labels: ["rpc", "state"]
             )

    p = %Participant{id: "a", peer_ref: peer}
    assert {:ok, state} = Backend.participant_joined(p, state)
    assert {:ok, state} = Backend.maybe_server_open(p, state)

    assert_receive {:opened, "rpc", opts_rpc, _}
    assert opts_rpc[:ordered] == true
    assert_receive {:opened, "state", opts_state, _}
    assert opts_state[:ordered] == false
    assert state.server_open_labels == ["rpc", "state"]
  end

  test "maybe_server_open no-ops when empty" do
    assert {:ok, peer} = StubPeer.start_link(self())

    assert {:ok, state} =
             Backend.init(%Session{id: "s1", tenant_id: "t1"}, mode: :fanout_except_sender)

    p = %Participant{id: "a", peer_ref: peer}
    assert {:ok, state} = Backend.participant_joined(p, state)
    assert {:ok, _} = Backend.maybe_server_open(p, state)
    refute_receive {:opened, _, _, _}, 50
  end

  test "maybe_server_open skips labels already open" do
    assert {:ok, peer} = StubPeer.start_link(self())

    assert {:ok, state} =
             Backend.init(%Session{id: "s1", tenant_id: "t1"},
               labels: ["rpc"],
               server_open_labels: ["rpc"]
             )

    p = %Participant{id: "a", peer_ref: peer}
    assert {:ok, state} = Backend.participant_joined(p, state)
    ref = make_ref()
    assert {:ok, state} = Backend.handle_datachannel_open(p, %{ref: ref, label: "rpc", id: 0}, state)
    assert {:ok, _} = Backend.maybe_server_open(p, state)
    refute_receive {:opened, _, _, _}, 50
  end
end
