defmodule Xylon.Datachannel.DoctestTest do
  use ExUnit.Case, async: false

  setup do
    Xylon.Catalogue.reset!()
    :ok
  end

  doctest Xylon.Datachannel
  doctest Xylon.Datachannel.Backend
  doctest Xylon.Datachannel.Codec.JSON
  doctest Xylon.Datachannel.Codec.Msgpack
  doctest Xylon.Datachannel.Mfu
end
