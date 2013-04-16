require 'rubygems'
require 'json'
require 'rubberband'
require 'configliere'
require 'multi_json'
require 'zlib'


# Example command:                                                                                                     #
# ./cardinality.rd --esIndex="my_index"  --esData="dump_file.json"                                                       #

Settings.use :commandline
Settings.use :config_block
Settings.define :esIndex
Settings.define :esData
Settings.resolve!

class Cardinality

  def initialize(json_file, index)
    @json_file = json_file
    @index = index
    @gz_input     = Zlib::GzipReader.open(json_file)
    @unique_values = {}
  end

# Parse Json dump, catch each index and associated fields, grab any new values, increment count of new values          #
  def count_cardinality()
    puts "counting cardinality"
    count, documents = 0, []
    @gz_input.each_line do |json|
      documents << MultiJson.load(json)
      documents.each do |doc|
          count += 1
          if count%10000 == 0
            puts "processed #{count} documents"
          end
          doc.each do |key, value|
            @unique_values[value] = key
          end
      end
    end
  end

# Display cardinality of each index                                                                                   #
  def output
    puts @unique_values.keys.size
  end

# Execute calculations                                                                                                 #
  def run
    count_cardinality
    output
  end
end

cardinality_ob = Cardinality.new(Settings.esData, Settings.esIndex)
cardinality_ob.run


