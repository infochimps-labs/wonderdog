require 'rubygems'
require 'configliere'
require 'json'
require 'multi_json'

#Settings.use :commandline
#Settings.use :config_block
#Settings.define :dump
#Settings.define :field
#Settings.resolve!


class Cardinality
  attr_accessor :fields

  def initialize(dump)
    @dump = dump
    @fields = {}
  end

  def get_value_counts
    File.open(@dump).each do |line|
      record = MultiJson.load(line)
      record.keys.each do |field|
        @fields[field] ||= Hash.new(0)
        @fields[field][record[field]] ||= Hash.new(0)
        @fields[field][record[field]] += 1
      end
    end
    puts @fields.inspect
  end

  def output
    @field.keys.each do |field|
      puts "#{field} has #{@fields[field].keys.size} values"
    end
  end
end

#card_ob = Cardinality.new("/home/missy/GitProjects/wonderdog/test/flight_count_20130405").get_value_counts
#puts card_ob.fields.inspect


