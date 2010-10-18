#!/usr/bin/env jruby

$: << '/home/jacob/Programming/wonderdog/lib'

require 'wonderdog'

tweet = {
  :foo      => "bar",
  :grobnitz => "baz"
}

indexer = Wonderdog::Indexer.new("twitter2", "tweet")
indexer.index_one_document(666, tweet)
#
# Do some indexing yo
#
indexer.close
