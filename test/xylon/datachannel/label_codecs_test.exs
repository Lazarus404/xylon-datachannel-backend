defmodule Xylon.Datachannel.LabelCodecsTest do
  use ExUnit.Case, async: false

  alias Xylon.Datachannel.Backend
  alias Xylon.Datachannel.Codec.Msgpack
  alias Xylon.{Participant, Session}

  test "label_codecs uses msgpack on state and json on chat" do
    session = %Session{id: "s1", tenant_id: "t1"}

    assert {:ok, state} =
             Backend.init(session,
               mode: :mfu,
               codec: Xylon.Datachannel.Codec.JSON,
               labels: ["chat", "state"],
               label_codecs: %{"state" => Msgpack},
               types: %{"chat" => :passthrough}
             )

    assert state.label_codecs["state"] == Msgpack
    assert state.dc_ppi == :string

    a = %Participant{id: "a", peer_ref: self()}
    b = %Participant{id: "b", peer_ref: self()}
    assert {:ok, state} = Backend.participant_joined(a, state)
    assert {:ok, state} = Backend.participant_joined(b, state)

    chat_a = make_ref()
    chat_b = make_ref()
    state_a = make_ref()
    state_b = make_ref()
    assert {:ok, state} = Backend.handle_datachannel_open(a, %{ref: chat_a, label: "chat", id: 0}, state)
    assert {:ok, state} = Backend.handle_datachannel_open(b, %{ref: chat_b, label: "chat", id: 1}, state)
    assert {:ok, state} = Backend.handle_datachannel_open(a, %{ref: state_a, label: "state", id: 2}, state)
    assert {:ok, state} = Backend.handle_datachannel_open(b, %{ref: state_b, label: "state", id: 3}, state)

    chat = ~s({"type":"chat","text":"hi"})
    assert {:forward_dc, [{^b, ^chat_b, ^chat}], state} =
             Backend.handle_datachannel(a, chat_a, chat, state)

    assert state.dc_ppi == :string

    assert {:ok, packed} = Msgpack.encode(%{"type" => "state", "topic" => "t", "seq" => 1, "body" => %{}})

    assert {:forward_dc, [{^b, ^state_b, out}], state} =
             Backend.handle_datachannel(a, state_a, packed, state)

    assert state.dc_ppi == :binary
    assert {:ok, decoded} = Msgpack.decode(out)
    assert decoded["type"] == "state"
  end
end
