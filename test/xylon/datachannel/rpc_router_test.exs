defmodule Xylon.Datachannel.RpcRouterTest do
  use ExUnit.Case, async: false

  alias Xylon.Datachannel.{Backend, RpcRouter}
  alias Xylon.{Participant, Session}

  test "rpc.req fans out only to to; rpc.res returns to caller" do
    session = %Session{id: "s1", tenant_id: "t1"}

    assert {:ok, state} =
             Backend.init(session,
               mode: :mfu,
               rpc_router: true,
               labels: ["rpc"]
             )

    a = %Participant{id: "a", peer_ref: self()}
    b = %Participant{id: "b", peer_ref: self()}
    c = %Participant{id: "c", peer_ref: self()}
    assert {:ok, state} = Backend.participant_joined(a, state)
    assert {:ok, state} = Backend.participant_joined(b, state)
    assert {:ok, state} = Backend.participant_joined(c, state)

    ref_a = make_ref()
    ref_b = make_ref()
    ref_c = make_ref()
    assert {:ok, state} = Backend.handle_datachannel_open(a, %{ref: ref_a, label: "rpc", id: 0}, state)
    assert {:ok, state} = Backend.handle_datachannel_open(b, %{ref: ref_b, label: "rpc", id: 1}, state)
    assert {:ok, state} = Backend.handle_datachannel_open(c, %{ref: ref_c, label: "rpc", id: 2}, state)

    req =
      ~s({"type":"rpc.req","id":"r1","method":"ping","to":"b","payload":{"n":1}})

    assert {:forward_dc, specs, state} = Backend.handle_datachannel(a, ref_a, req, state)
    assert length(specs) == 1
    assert [{^b, ^ref_b, bin}] = specs
    assert {:ok, decoded} = Jason.decode(bin)
    assert decoded["from"] == "a"
    assert decoded["to"] == "b"

    res = ~s({"type":"rpc.res","id":"r1","payload":{"pong":true}})
    assert {:forward_dc, specs2, _} = Backend.handle_datachannel(b, ref_b, res, state)
    assert [{^a, ^ref_a, _}] = specs2
  end

  test "mfu meta includes sender_id and label" do
    session = %Session{id: "s1", tenant_id: "t1"}
    test_pid = self()

    assert {:ok, state} =
             Backend.init(session,
               mode: :mfu,
               types: %{
                 "probe" => fn _decoded, meta ->
                   send(test_pid, {:meta, meta})
                   :drop
                 end
               }
             )

    a = %Participant{id: "alice", peer_ref: self()}
    assert {:ok, state} = Backend.participant_joined(a, state)
    ref = make_ref()
    assert {:ok, state} = Backend.handle_datachannel_open(a, %{ref: ref, label: "chat", id: 0}, state)

    assert {:ok, _} = Backend.handle_datachannel(a, ref, ~s({"type":"probe"}), state)
    assert_receive {:meta, meta}
    assert meta.sender_id == "alice"
    assert meta.label == "chat"
  end

  test "init with rpc_router merges types and chat/state handlers" do
    session = %Session{id: "s1", tenant_id: "t1"}
    assert {:ok, state} = Backend.init(session, mode: :mfu, rpc_router: true)
    assert is_function(state.types["rpc.req"])
    assert state.types["chat"] == :passthrough
    assert is_function(state.types["state"])
  end

  test "clear_participant removes pending" do
    plan = make_ref()
    RpcRouter.ensure_table!()
    types = RpcRouter.types_map(plan, [])
    meta = %{sender_id: "a", label: "rpc", session_id: "s", tenant_id: "t", mode: :mfu}

    assert {:forward, _, ["b"]} =
             types["rpc.req"].(
               %{
                 "type" => "rpc.req",
                 "id" => "x1",
                 "method" => "m",
                 "to" => "b",
                 "payload" => %{}
               },
               meta
             )

    assert :ok = RpcRouter.clear_participant(plan, "a")

    assert :drop =
             types["rpc.res"].(%{"type" => "rpc.res", "id" => "x1", "payload" => %{}}, meta)
  end

  test "rpc.req to star fans out except_sender; first res wins" do
    session = %Session{id: "s1", tenant_id: "t1"}

    assert {:ok, state} =
             Backend.init(session,
               mode: :mfu,
               rpc_router: true,
               labels: ["rpc"]
             )

    a = %Participant{id: "a", peer_ref: self()}
    b = %Participant{id: "b", peer_ref: self()}
    c = %Participant{id: "c", peer_ref: self()}
    assert {:ok, state} = Backend.participant_joined(a, state)
    assert {:ok, state} = Backend.participant_joined(b, state)
    assert {:ok, state} = Backend.participant_joined(c, state)

    ref_a = make_ref()
    ref_b = make_ref()
    ref_c = make_ref()
    assert {:ok, state} = Backend.handle_datachannel_open(a, %{ref: ref_a, label: "rpc", id: 0}, state)
    assert {:ok, state} = Backend.handle_datachannel_open(b, %{ref: ref_b, label: "rpc", id: 1}, state)
    assert {:ok, state} = Backend.handle_datachannel_open(c, %{ref: ref_c, label: "rpc", id: 2}, state)

    req = ~s({"type":"rpc.req","id":"bcast","method":"ping","to":"*","payload":{}})
    assert {:forward_dc, specs, state} = Backend.handle_datachannel(a, ref_a, req, state)
    assert length(specs) == 2
    dest_ids = Enum.map(specs, fn {p, _, _} -> p.id end) |> Enum.sort()
    assert dest_ids == ["b", "c"]

    res_b = ~s({"type":"rpc.res","id":"bcast","payload":{"from":"b"}})
    assert {:forward_dc, [{^a, ^ref_a, _}], state} =
             Backend.handle_datachannel(b, ref_b, res_b, state)

    res_c = ~s({"type":"rpc.res","id":"bcast","payload":{"from":"c"}})
    assert {:ok, _} = Backend.handle_datachannel(c, ref_c, res_c, state)
  end

  test "rpc.req to id list fans out to those ids only" do
    session = %Session{id: "s1", tenant_id: "t1"}

    assert {:ok, state} =
             Backend.init(session,
               mode: :mfu,
               rpc_router: true,
               labels: ["rpc"]
             )

    a = %Participant{id: "a", peer_ref: self()}
    b = %Participant{id: "b", peer_ref: self()}
    c = %Participant{id: "c", peer_ref: self()}
    assert {:ok, state} = Backend.participant_joined(a, state)
    assert {:ok, state} = Backend.participant_joined(b, state)
    assert {:ok, state} = Backend.participant_joined(c, state)

    ref_a = make_ref()
    ref_b = make_ref()
    ref_c = make_ref()
    assert {:ok, state} = Backend.handle_datachannel_open(a, %{ref: ref_a, label: "rpc", id: 0}, state)
    assert {:ok, state} = Backend.handle_datachannel_open(b, %{ref: ref_b, label: "rpc", id: 1}, state)
    assert {:ok, state} = Backend.handle_datachannel_open(c, %{ref: ref_c, label: "rpc", id: 2}, state)

    req = ~s({"type":"rpc.req","id":"m1","method":"ping","to":["b","c"],"payload":{}})
    assert {:forward_dc, specs, _} = Backend.handle_datachannel(a, ref_a, req, state)
    dest_ids = Enum.map(specs, fn {p, _, _} -> p.id end) |> Enum.sort()
    assert dest_ids == ["b", "c"]
  end
end
