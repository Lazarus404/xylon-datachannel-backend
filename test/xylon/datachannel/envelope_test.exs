defmodule Xylon.Datachannel.EnvelopeTest do
  use ExUnit.Case, async: true
  doctest Xylon.Datachannel.Envelope

  alias Xylon.Datachannel.Envelope

  test "validate rpc.req" do
    assert {:ok, %{type: "rpc.req", id: "i1", method: "ping", to: {:unicast, "b"}, payload: %{}}} =
             Envelope.validate(%{
               "type" => "rpc.req",
               "id" => "i1",
               "method" => "ping",
               "to" => "b",
               "payload" => %{}
             })
  end

  test "validate rpc.req accepts star and id list" do
    assert {:ok, %{to: :except_sender}} =
             Envelope.validate(%{
               "type" => "rpc.req",
               "id" => "i1",
               "method" => "ping",
               "to" => "*",
               "payload" => %{}
             })

    assert {:ok, %{to: {:ids, ["b", "c"]}}} =
             Envelope.validate(%{
               "type" => "rpc.req",
               "id" => "i2",
               "method" => "ping",
               "to" => ["b", "c", "b"],
               "payload" => %{}
             })
  end

  test "validate rpc.req rejects missing fields" do
    assert {:error, :invalid} = Envelope.validate(%{"type" => "rpc.req", "id" => "i1"})
  end

  test "dest_ids treats star as literal participant id not room broadcast" do
    assert Envelope.dest_ids(%{"to" => "*"}) == {:ok, ["*"]}
  end

  test "validate rpc.res and rpc.err" do
    assert {:ok, %{type: "rpc.res", id: "i1", payload: %{"ok" => true}}} =
             Envelope.validate(%{"type" => "rpc.res", "id" => "i1", "payload" => %{"ok" => true}})

    assert {:ok, %{type: "rpc.err", id: "i1", code: "x", message: "nope"}} =
             Envelope.validate(%{
               "type" => "rpc.err",
               "id" => "i1",
               "code" => "x",
               "message" => "nope"
             })
  end

  test "validate state and chat" do
    assert {:ok, %{type: "state", topic: "t", seq: 1, body: %{}}} =
             Envelope.validate(%{"type" => "state", "topic" => "t", "seq" => 1, "body" => %{}})

    assert {:ok, %{type: "chat", text: "hi"}} =
             Envelope.validate(%{"type" => "chat", "text" => "hi"})
  end

  test "extract helpers" do
    map = %{"type" => "rpc.req", "id" => "i1", "to" => "b", "method" => "m", "payload" => 1}
    assert Envelope.id(map) == "i1"
    assert Envelope.to(map) == "b"
    assert Envelope.seq(%{"seq" => 42}) == 42
  end

  test "dest_ids omits missing or nil to" do
    assert Envelope.dest_ids(%{"type" => "chat", "text" => "hi"}) == :omit
    assert Envelope.dest_ids(%{"to" => nil}) == :omit
  end

  test "dest_ids accepts string or list of ids" do
    assert Envelope.dest_ids(%{"to" => "b"}) == {:ok, ["b"]}
    assert Envelope.dest_ids(%{"to" => ["b", "c", "b"]}) == {:ok, ["b", "c"]}
  end

  test "dest_ids rejects filters and bad shapes" do
    assert Envelope.dest_ids(%{"to" => %{"role" => "mod"}}) == :reject
    assert Envelope.dest_ids(%{"to" => []}) == :reject
    assert Envelope.dest_ids(%{"to" => ""}) == :reject
    assert Envelope.dest_ids(%{"to" => ["b", ""]}) == :reject
    assert Envelope.dest_ids(%{"to" => ["b", 1]}) == :reject
  end
end
