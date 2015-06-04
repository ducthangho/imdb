#!/usr/bin/env bash
#!/bin/bash
 g++ -std=gnu++1y -O2 -Wall apps/imdb/hash-server.c++ apps/imdb/hashprotocol.capnp.cc \
  -lcapnpc -lcapnp-rpc -lcapnp -lkj-async -lkj -o hash-server


# g++ -std=gnu++1y -O2 -Wall -D_GLIBCXX_USE_CXX11_ABI=0  apps/imdb/hash-client.c++ apps/imdb/hashprotocol.capnp.cc \
#     -lcapnpc -lcapnp-rpc -lcapnp -lkj-async -lkj -o hash-client
