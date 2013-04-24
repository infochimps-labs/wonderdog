require 'rubygems'
require 'configliere'
require 'json'
require 'multi_json'

Settings.use :commandline
Settings.use :config_block
Settings.define :dump
Settings.define :field
Settings.resolve!

def get_value_counts(dump, field)
  File.open(dump).each do |line|
    record = MultiJson.load(line)
    puts record[field]
  end
end

get_value_counts(Settings.dump, Settings.field)