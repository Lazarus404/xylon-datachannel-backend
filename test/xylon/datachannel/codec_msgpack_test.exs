defmodule Xylon.Datachannel.Codec.MsgpackTest do
  use ExUnit.Case, async: true

  alias Xylon.Datachannel.Codec.Msgpack
  alias Xylon.Datachannel.Backend
  alias Xylon.{Participant, Session}

  test "round-trip and type_key" do
    map = %{"type" => "chat", "text" => "hi"}
    assert {:ok, bin} = Msgpack.encode(map)
    assert is_binary(bin)
    assert {:ok, decoded} = Msgpack.decode(bin)
    assert decoded["type"] == "chat"
    assert decoded["text"] == "hi"
    assert Msgpack.type_key(decoded) == "chat"
  end

  test "decode error" do
    assert {:error, _} = Msgpack.decode(<<0xC1>>)
  end

  test "mfu with msgpack codec fans out chat" do
    session = %Session{id: "s1", tenant_id: "t1"}

    assert {:ok, state} =
             Backend.init(session,
               mode: :mfu,
               codec: Msgpack,
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

    assert {:ok, payload} = Msgpack.encode(%{"type" => "chat", "text" => "hi"})

    assert {:forward_dc, [{^b, ^ref_b, out}], _} =
             Backend.handle_datachannel(a, ref_a, payload, state)

    assert {:ok, decoded} = Msgpack.decode(out)
    assert decoded["type"] == "chat"
    assert decoded["text"] == "hi"
  end

  test "mfu msgpack state stamp round-trip" do
    session = %Session{id: "s1", tenant_id: "t1"}

    assert {:ok, state} =
             Backend.init(session,
               mode: :mfu,
               codec: Msgpack,
               labels: ["state"],
               default_label: "state"
             )

    assert state.dc_ppi == :binary
    assert state.codec == Msgpack

    a = %Participant{id: "a", peer_ref: self()}
    b = %Participant{id: "b", peer_ref: self()}
    assert {:ok, state} = Backend.participant_joined(a, state)
    assert {:ok, state} = Backend.participant_joined(b, state)

    ref_a = make_ref()
    ref_b = make_ref()

    assert {:ok, state} =
             Backend.handle_datachannel_open(a, %{ref: ref_a, label: "state", id: 0}, state)

    assert {:ok, state} =
             Backend.handle_datachannel_open(b, %{ref: ref_b, label: "state", id: 1}, state)

    assert {:ok, payload} =
             Msgpack.encode(%{
               "type" => "state",
               "topic" => "player",
               "seq" => 1,
               "t" => 0,
               "body" => %{"x" => 1}
             })

    assert {:forward_dc, [{^b, ^ref_b, out}], _} =
             Backend.handle_datachannel(a, ref_a, payload, state)

    assert {:ok, decoded} = Msgpack.decode(out)
    assert decoded["type"] == "state"
    assert decoded["from"] == "a"
    assert decoded["seq"] == 1
  end
end
