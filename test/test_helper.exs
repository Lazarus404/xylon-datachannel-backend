ExUnit.start(exclude: [:dual_node])

Application.put_env(:xylon, :ice, advertised_ip: {127, 0, 0, 1})
