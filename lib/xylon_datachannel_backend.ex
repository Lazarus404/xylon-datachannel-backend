defmodule XylonDatachannelBackend do
  @moduledoc """
  OTP application wrapper for the Xylon datachannel media library.

  ## What problem this solves

  Hex/OTP packaging needs an app module matching `:xylon_datachannel_backend`.
  Product attach and publish live on `Xylon.Datachannel` and
  `Xylon.Datachannel.Backend`; use those modules from host apps.

  ## RFCs

  - Packaging only; see `Xylon.Datachannel` for wire RFCs
  """
end
