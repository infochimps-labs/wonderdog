require 'configliere'
require 'json'

Settings.use :commandline
Settings.use :config_block
Settings.define :es_index, default: nil
Settings.define :fields, default: nil
Settings.define :one_index, default: false, flag: 'o'
Settings.define :composite_key, default: nil
Settings.define :es_type
Settings.finally do |settings|
  settings.es_index ||= settings.one_index ? [settings.es_type, "cnt"].join : 'ad_activity'
end
Settings.resolve!

puts "using es index #{Settings.es_index} and type #{Settings.es_type}"

index = Settings.es_index 
type = Settings.es_type 

response = `curl localhost:9200/#{Settings.es_index}/_mapping/`
puts JSON.parse(response)["#{index}"]["#{type}"]#["properties"]
fields = JSON.parse(response)["#{index}"]["#{type}"]["properties"].keys#.select{|x| x.end_with?("_id")}
#fields += %w[metric feature]# browser_ua]
#fields = JSON.parse(response)["#{type}_legacy"][type]["properties"].keys.select{|x| x.end_with?("_id")}

puts "got fields #{Settings.fields || fields}"

# site_count => site_composite
# placement_count => pl_composite
# flight_count => metric_feature

composite_key = case Settings.es_type
                when "site_count" then "site_composite"
                when "placement_count" then "pl_composite"
                when "flight_count" then "metric_feature"
                else nil
                end

doc = {
  "query"=> {
    "match_all"=> {}
  },
  "facets"=> {
    # "ignz"=> {
    #   "statistical"=> {
    #     "field" => "cnt"
    #   }
    # },
    "igna"=> {
      "date_histogram"=> {
        "field"=> "tb_h",
	"interval" => "day"
      }
    },
    "ignb"=> {
      "terms"=> {
        "fields"=> [Settings.fields || fields].flatten
      }
    },
  }
}

fields.each do |field|
  doc["facets"]["ign_#{field}"] = {
    "terms_stats" => {
      "key_field" => field,
      "value_field" => "cnt"
    }
  }
end

if not composite_key.nil?
  doc["facets"]["ignc"] = {
    "terms_stats" => {
      "key_field" => Settings.composite_key || composite_key,
      "value_field" => "cnt"
    }
  }
end

puts "about to query with " + doc.to_s

results = `curl localhost:9200/#{Settings.es_index}/#{Settings.es_type}/_search/?pretty=true -d '#{JSON.generate(doc)}'`
term_hash = {}
resultsLineArray = results.split( /\r?\n/ )
copy_resultsLineArray = resultsLineArray - []
copy_resultsLineArray.each_with_index do |line, index|
  if line.include?("\"term\" : \"")
	term_hash[line] = index
  end
end
puts `curl localhost:9200/#{Settings.es_index}/#{Settings.es_type}/_search/?pretty=true -d '#{JSON.generate(doc)}'`
puts "from command: \n curl localhost:9200/#{Settings.es_index}/#{Settings.es_type}/_search/ -d '#{JSON.generate(doc)}'"
puts "number of terms: #{term_hash.keys.size()}"
