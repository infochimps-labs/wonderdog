#!/usr/bin/env jruby

$: << '/home/jacob/Programming/wonderdog/lib'

require 'wonderdog'
require 'wonderdog/client'
require 'wonderdog/indexer'

node   = NodeBuilder.node_builder.node
client = node.client

index = ["_all"].to_java(:string)
p client.admin.indices.prepare_refresh(index).execute
node.close
