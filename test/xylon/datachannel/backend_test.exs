defmodule Xylon.Datachannel.BackendTest do
  use ExUnit.Case, async: true

  alias Xylon.Datachannel.Backend
  alias Xylon.{Participant, Session}

  test "unknown_channel heals by registering inbound ref under chat when empty" do
    session = %Session{id: "s-heal", tenant_id: "t1"}
    assert {:ok, state} = Backend.init(session, mode: :fanout_except_sender)

    a = %Participant{id: "a", peer_ref: self()}
    b = %Participant{id: "b", peer_ref: self()}
    assert {:ok, state} = Backend.participant_joined(a, state)
    assert {:ok, state} = Backend.participant_joined(b, state)

    ref_b = make_ref()
    assert {:ok, state} = Backend.handle_datachannel_open(b, %{ref: ref_b, label: "chat", id: 1}, state)

    # Sender never got handle_datachannel_open (Session race); inbound ref unknown.
    ref_a = make_ref()

    assert {:forward_dc, [{^b, ^ref_b, "hello"}], state2} =
             Backend.handle_datachannel(a, ref_a, "hello", state)

    assert Xylon.Datachannel.Plan.label_for_ref(state2.plan_ref, "a", ref_a) == "chat"
  end

  test "fanout_except_sender forwards to peers with matching label" do
    session = %Session{id: "s1", tenant_id: "t1"}
    assert {:ok, state} = Backend.init(session, mode: :fanout_except_sender)

    a = %Participant{id: "a", peer_ref: self()}
    b = %Participant{id: "b", peer_ref: self()}
    assert {:ok, state} = Backend.participant_joined(a, state)
    assert {:ok, state} = Backend.participant_joined(b, state)

    ref_a = make_ref()
    ref_b = make_ref()
    assert {:ok, state} = Backend.handle_datachannel_open(a, %{ref: ref_a, label: "chat", id: 0}, state)
    assert {:ok, state} = Backend.handle_datachannel_open(b, %{ref: ref_b, label: "chat", id: 1}, state)

    assert {:forward_dc, specs, _state} =
             Backend.handle_datachannel(a, ref_a, "hello", state)

    assert length(specs) == 1
    assert [{^b, ^ref_b, "hello"}] = specs
  end

  test "mfu passthrough type fans out" do
    session = %Session{id: "s1", tenant_id: "t1"}

    assert {:ok, state} =
             Backend.init(session,
               mode: :mfu,
               types: %{"chat" => :passthrough}
             )

    a = %Participant{id: "a", peer_ref: self()}
    b = %Participant{id: "b", peer_ref: self()}
    assert {:ok, state} = Backend.participant_joined(a, state)
    assert {:ok, state} = Backend.participant_joined(b, state)

    ref_a = make_ref()
    ref_b = make_ref()
    assert {:ok, state} = Backend.handle_datachannel_open(a, %{ref: ref_a, label: "data", id: 0}, state)
    assert {:ok, state} = Backend.handle_datachannel_open(b, %{ref: ref_b, label: "data", id: 1}, state)

    payload = ~s({"type":"chat","text":"hi"})

    assert {:forward_dc, [{^b, ^ref_b, ^payload}], _} =
             Backend.handle_datachannel(a, ref_a, payload, state)
  end

  test "publish_datachannel builds specs for :all" do
    session = %Session{id: "s1", tenant_id: "t1"}
    assert {:ok, state} = Backend.init(session, default_label: "chat")

    a = %Participant{id: "a", peer_ref: self()}
    b = %Participant{id: "b", peer_ref: self()}
    assert {:ok, state} = Backend.participant_joined(a, state)
    assert {:ok, state} = Backend.participant_joined(b, state)

    ref_a = make_ref()
    ref_b = make_ref()
    assert {:ok, state} = Backend.handle_datachannel_open(a, %{ref: ref_a, label: "chat", id: 0}, state)
    assert {:ok, state} = Backend.handle_datachannel_open(b, %{ref: ref_b, label: "chat", id: 1}, state)

    assert {:forward_dc, specs, _} = Backend.publish_datachannel("ping", :all, [], state)
    assert length(specs) == 2
  end

  test "oversized messages are dropped" do
    session = %Session{id: "s1", tenant_id: "t1"}
    assert {:ok, state} = Backend.init(session, max_message_bytes: 4)

    a = %Participant{id: "a", peer_ref: self()}
    assert {:ok, state} = Backend.participant_joined(a, state)
    ref = make_ref()
    assert {:ok, state} = Backend.handle_datachannel_open(a, %{ref: ref, label: "chat", id: 0}, state)

    assert {:ok, _} = Backend.handle_datachannel(a, ref, "too-big", state)
  end

  test "forward_plan/2 returns datachannel mode plan" do
    session = %Session{id: "s1", tenant_id: "t1"}
    assert {:ok, state} = Backend.init(session, [])
    a = %Participant{id: "a", peer_ref: self()}

    assert {:ok, plan, ^state} = Backend.forward_plan(a, state)
    assert %{mode: :datachannel, participant: ^a} = plan
  end

  test "participant_joined stores Participant in Plan ETS only" do
    session = %Session{id: "s1", tenant_id: "t1"}
    assert {:ok, state} = Backend.init(session, [])
    a = %Participant{id: "a", peer_ref: self()}
    assert {:ok, state} = Backend.participant_joined(a, state)

    assert %{"a" => ^a} = Xylon.Datachannel.Plan.participants_map(state.plan_ref)
    refute Map.has_key?(state, :participants)
  end

  test "mfu emit fans out payloads in emit order" do
    session = %Session{id: "s1", tenant_id: "t1"}

    assert {:ok, state} =
             Backend.init(session,
               mode: :mfu,
               types: %{
                 "burst" => fn _decoded, _meta ->
                   {:emit, ["one", "two"]}
                 end
               }
             )

    a = %Participant{id: "a", peer_ref: self()}
    b = %Participant{id: "b", peer_ref: self()}
    assert {:ok, state} = Backend.participant_joined(a, state)
    assert {:ok, state} = Backend.participant_joined(b, state)

    ref_a = make_ref()
    ref_b = make_ref()
    assert {:ok, state} = Backend.handle_datachannel_open(a, %{ref: ref_a, label: "data", id: 0}, state)
    assert {:ok, state} = Backend.handle_datachannel_open(b, %{ref: ref_b, label: "data", id: 1}, state)

    payload = ~s({"type":"burst"})

    assert {:forward_dc, specs, _} = Backend.handle_datachannel(a, ref_a, payload, state)
    assert [{^b, ^ref_b, "one"}, {^b, ^ref_b, "two"}] = specs
  end

  test "authorize meta includes participant capabilities" do
    test_pid = self()

    assert {:ok, state} =
             Backend.init(%Session{id: "s1", tenant_id: "t1"},
               mode: :fanout_except_sender,
               authorize: fn meta ->
                 send(test_pid, {:auth, meta})
                 true
               end
             )

    a = %Participant{id: "a", peer_ref: self(), meta: %{capabilities: ["send:chat"]}}
    b = %Participant{id: "b", peer_ref: self()}
    assert {:ok, state} = Backend.participant_joined(a, state)
    assert {:ok, state} = Backend.participant_joined(b, state)
    ref_a = make_ref()
    ref_b = make_ref()
    assert {:ok, state} = Backend.handle_datachannel_open(a, %{ref: ref_a, label: "chat", id: 0}, state)
    assert {:ok, state} = Backend.handle_datachannel_open(b, %{ref: ref_b, label: "chat", id: 1}, state)

    assert {:forward_dc, [_], _} = Backend.handle_datachannel(a, ref_a, "hi", state)
    assert_receive {:auth, meta}
    assert meta.capabilities == ["send:chat"]
    assert meta.dir == :send
    assert meta.label == "chat"
  end

  test "authorize denies send without send:label cap" do
    deny_unless =
      fn meta ->
        caps = Map.get(meta, :capabilities, [])
        "send:chat" in caps or "send" in caps
      end

    assert {:ok, state} =
             Backend.init(%Session{id: "s1", tenant_id: "t1"},
               mode: :fanout_except_sender,
               authorize: deny_unless
             )

    a = %Participant{id: "a", peer_ref: self(), meta: %{capabilities: ["send:rpc"]}}
    b = %Participant{id: "b", peer_ref: self()}
    assert {:ok, state} = Backend.participant_joined(a, state)
    assert {:ok, state} = Backend.participant_joined(b, state)
    ref_a = make_ref()
    ref_b = make_ref()
    assert {:ok, state} = Backend.handle_datachannel_open(a, %{ref: ref_a, label: "chat", id: 0}, state)
    assert {:ok, state} = Backend.handle_datachannel_open(b, %{ref: ref_b, label: "chat", id: 1}, state)

    assert {:forward_dc, [], _} = Backend.handle_datachannel(a, ref_a, "hi", state)
  end

  test "authorize meta includes envelope type for JSON payloads" do
    test_pid = self()

    assert {:ok, state} =
             Backend.init(%Session{id: "s1", tenant_id: "t1"},
               mode: :fanout_except_sender,
               authorize: fn meta ->
                 send(test_pid, {:auth, meta})
                 true
               end
             )

    a = %Participant{id: "a", peer_ref: self()}
    b = %Participant{id: "b", peer_ref: self()}
    assert {:ok, state} = Backend.participant_joined(a, state)
    assert {:ok, state} = Backend.participant_joined(b, state)
    ref_a = make_ref()
    ref_b = make_ref()
    assert {:ok, state} = Backend.handle_datachannel_open(a, %{ref: ref_a, label: "chat", id: 0}, state)
    assert {:ok, state} = Backend.handle_datachannel_open(b, %{ref: ref_b, label: "chat", id: 1}, state)

    assert {:forward_dc, [_], _} =
             Backend.handle_datachannel(a, ref_a, ~s({"type":"chat","text":"hi"}), state)

    assert_receive {:auth, meta}
    assert meta.type == "chat"
  end

  test "authorize arity-2 fun uses dir as action" do
    assert {:ok, state} =
             Backend.init(%Session{id: "s1", tenant_id: "t1"},
               mode: :fanout_except_sender,
               authorize: fn
                 :send, _meta -> false
                 _action, _meta -> true
               end
             )

    a = %Participant{id: "a", peer_ref: self()}
    b = %Participant{id: "b", peer_ref: self()}
    assert {:ok, state} = Backend.participant_joined(a, state)
    assert {:ok, state} = Backend.participant_joined(b, state)
    ref_a = make_ref()
    ref_b = make_ref()
    assert {:ok, state} = Backend.handle_datachannel_open(a, %{ref: ref_a, label: "chat", id: 0}, state)
    assert {:ok, state} = Backend.handle_datachannel_open(b, %{ref: ref_b, label: "chat", id: 1}, state)

    assert {:forward_dc, [], _} = Backend.handle_datachannel(a, ref_a, "hi", state)
  end
end
