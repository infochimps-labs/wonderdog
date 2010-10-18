#!/usr/bin/env jruby

$: << '/home/jacob/Programming/wonderdog/lib'

require 'wonderdog'

client = Wonderdog::Client.new
p client.cluster_name
client.close
