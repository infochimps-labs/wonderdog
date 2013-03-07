# -*- encoding: utf-8 -*-
require File.expand_path('../lib/wonderdog/version', __FILE__)

Gem::Specification.new do |gem|
  gem.name        = 'wonderdog'
  gem.homepage    = 'https://github.com/infochimps-labs/wonderdog'
  gem.licenses    = ["Apache 2.0"]
  gem.email       = 'coders@infochimps.com'
  gem.authors     = ['Infochimps', 'Philip (flip) Kromer', 'Jacob Perkins', 'Travis Dempsey', 'Dhruv Bansal']
  gem.version     = Wonderdog::VERSION

  gem.summary     = 'Make Hadoop and ElasticSearch play together nicely.'
  gem.description = <<-EOF
  Wonderdog provides code in both Ruby and Java to make Elasticsearch
  a more fully-fledged member of both the Hadoop and Wukong
  ecosystems.

  For the Java side, Wonderdog provides InputFormat and OutputFormat
  classes for use with Hadoop (esp. Hadoop Streaming) and Pig.

  For the Ruby side, Wonderdog provides extensions for wu-hadoop to
  make running Hadoop Streaming jobs written in Wukong against
  ElasticSearch easier.
EOF

  gem.files         = `git ls-files`.split("\n")
  gem.executables   = []
  gem.test_files    = gem.files.grep(/^spec/)
  gem.require_paths = ['lib']

  gem.add_dependency('wukong-hadoop', '0.1.1')
end
