require 'rubygems'
require 'configliere'
require 'json'
require 'multi_json'

Settings.use :commandline
Settings.use :config_block
Settings.define :dump
Settings.define :field
Settings.resolve!

@fields = {}
def get_value_counts(dump)
  File.open(dump).each do |line|
    record = MultiJson.load(line)
    record.keys.each do |field|
      @fields[field] ||= Hash.new(0)
      @fields[field][record[field]] ||= Hash.new(0)
      @fields[field][record[field]] += 1
    end
  end
end

get_value_counts(Settings.dump)
puts @field_names.inspect
@field.keys.each do |field|
  puts @fields[field].keys.size
end
