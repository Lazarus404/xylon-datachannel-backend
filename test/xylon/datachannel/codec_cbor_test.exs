defmodule Xylon.Datachannel.Codec.CborTest do
  use ExUnit.Case, async: true
  doctest Xylon.Datachannel.Codec.Cbor

  alias Xylon.Datachannel.Codec.Cbor
  alias Xylon.Datachannel.Backend
  alias Xylon.{Participant, Session}

  test "round-trip and type_key" do
    map = %{"type" => "chat", "text" => "hi"}
    assert {:ok, bin} = Cbor.encode(map)
    assert is_binary(bin)
    assert {:ok, decoded} = Cbor.decode(bin)
    assert decoded["type"] == "chat"
    assert decoded["text"] == "hi"
    assert Cbor.type_key(decoded) == "chat"
  end

  test "mfu with cbor codec sets binary ppi and fans out" do
    session = %Session{id: "s1", tenant_id: "t1"}

    assert {:ok, state} =
             Backend.init(session,
               mode: :mfu,
               codec: Cbor,
               types: %{"chat" => :passthrough}
             )

    assert state.dc_ppi == :binary

    a = %Participant{id: "a", peer_ref: self()}
    b = %Participant{id: "b", peer_ref: self()}
    assert {:ok, state} = Backend.participant_joined(a, state)
    assert {:ok, state} = Backend.participant_joined(b, state)

    ref_a = make_ref()
    ref_b = make_ref()
    assert {:ok, state} = Backend.handle_datachannel_open(a, %{ref: ref_a, label: "data", id: 0}, state)
    assert {:ok, state} = Backend.handle_datachannel_open(b, %{ref: ref_b, label: "data", id: 1}, state)

    assert {:ok, payload} = Cbor.encode(%{"type" => "chat", "text" => "hi"})

    assert {:forward_dc, [{^b, ^ref_b, out}], _} =
             Backend.handle_datachannel(a, ref_a, payload, state)

    assert {:ok, decoded} = Cbor.decode(out)
    assert decoded["type"] == "chat"
  end
end
