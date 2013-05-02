#! /usr/bin/ruby

require "configliere"
require_relative "../squirrel/squirrel.rb"

doc = <<DOC
This is the uber script the arguements you give it decide what happens
squirrel => Standard Query Ultracrepidate Iamatology Ruby Resource for Elasticsearch Labarum ##
  example commands:
    clear all caches
      ruby squirrel.rb cache --host=localhost --port=9200 --clear_all_cache=true
    run slow log queries
      ruby squirrel.rb replay --host=localhost --port=9200 --execute_slow_queries=/var/log/elasticsearch/padraig.log
    get backup an index aka generate a dumpfile
      ruby squirrel.rb backup --host=localhost --port=9200 --output_dir="." --dump_index=flight_count_20130405 --batch_size=100 --dump_mapping=flight_count_20130405_mapping.json
    get the cardinality of a dumpfile(card_file)
      ruby squirrel.rb cardinality --host=localhost --port=9200 --output_dir="." --card_file=flight_count_20130405 --cardinality=cnt,metric
    restore an index from a dumpfile
      ruby squirrel.rb restore --host=localhost --port=9200 --output_dir="." --restore_file=flight_count_20130405.gz --restore_index=flight_count_20130405 --restore_mapping=flight_count_20130405_mapping.json --batch_size=100
    duplicate files in an index from a dumpfile(duplicate_file)
      ruby squirrel.rb duplicate --host=localhost --port=9200 --output_dir="." --duplicate_file=flight_count_20130405.gz --duplicate_index=eight_flight_count_20130405 --duplicate_mapping=flight_count_20130405_mapping.json --batch_size=100
    add warmer
      ruby squirrel.rb warmer --host=localhost --port=9200 --output_dir="." --new_warmers_name=polite_warmer --warmers_index=flight_count_20130408 --create_warmer='{"sort" : ["_state", "flight_id","metric", "tb_h", "feature", "seconds", "base_feature", "metric_feature", "cnt", "_score"],"query":{"match_all":{}}}'
    remove warmer
      ruby squirrel.rb warmer --host=localhost --port=9200 --output_dir="." --remove_warmer=polite_warmer --warmers_index=flight_count_20130408
    disable warmers
      ruby squirrel.rb warmer --host=localhost --port=9200 --output_dir="." --warmers=false --warmers_index=flight_count_20130405
    enable warmers
      ruby squirrel.rb warmer --host=localhost --port=9200 --output_dir="." --warmers=false --warmers_index=flight_count_20130405
    remove warmer
      ruby squirrel.rb warmer --host=localhost --port=9200 --output_dir="." --remove_warmer=polite_warmer --warmers_index=flight_count_20130405
    change index settings
      ruby squirrel.rb index_settings --host=localhost --port=9200 --output_dir="." --settings_index=flight_count_20130405 --es_index_settings=refresh_interval,refresh_interval --es_index_settings_values=-1,0
DOC

Settings.use :commandline
Settings.use :commands
Settings.description = doc
Settings.define_command :backup, :description => "Create a dump gzip file of an index" do |cmd|
  cmd.define :output_dir,                     :default => nil,  :description => 'Directory to put output, defaults to nil'
  cmd.define :dump_file,                      :default => nil,  :description => 'The name of the dumpfile to use, default is nil'
  cmd.define :dump_index,                     :default => nil,  :description => 'Index to use, default is nil'
  cmd.define :query,                          :default => nil,  :description => 'Query to use in order to limit the data extracted from the index, default nil'
  cmd.define :host,                           :default => nil,  :description => 'The host to connect to, defaults to nil'
  cmd.define :port,         :type => Integer, :default => nil,  :description => 'The port to connect to on the host, defaults to nil'
  cmd.define :dump_mapping,                   :default => nil,  :description => 'The file to put the json mapping in, defaults to nil'
  cmd.define :batch_size,   :type => Integer, :default => nil,  :description => 'The number of lines to process at once, defaults to nil'
end
Settings.define_command :restore, :description => "Take the data from a dump gzip file and use it to populate an index" do |cmd|
  cmd.define :output_dir,                         :default => nil,  :description => 'Directory to put output, defaults to nil'
  cmd.define :restore_file,                       :default => nil,  :description => 'The name of the dumpfile to use, default is nil'
  cmd.define :restore_index,                      :default => nil,  :description => 'Index to use, default is nil'
  cmd.define :host,                               :default => nil,  :description => 'The host to connect to, defaults to nil'
  cmd.define :port,             :type => Integer, :default => nil,  :description => 'The port to connect to on the host, defaults to nil'
  cmd.define :restore_mapping,                    :default => nil,  :description => 'The mapping file to use when restoring an index, defaults to nil'
  cmd.define :batch_size,       :type => Integer, :default => nil,  :description => 'The number of lines to process at once, defaults to nil'
end
Settings.define_command :duplicate, :description => "Take the data from a dump gzip file and add it into an index allowing documents to be duplicated" do |cmd|
  cmd.define :output_dir,                           :default => nil,  :description => 'Directory to put output, defaults to nil'
  cmd.define :duplicate_file,                       :default => nil,  :description => 'The name of the dumpfile to use, default is nil'
  cmd.define :duplicate_index,                      :default => nil,  :description => 'Index to use, default is nil'
  cmd.define :host,                                 :default => nil,  :description => 'The host to connect to, defaults to nil'
  cmd.define :port,               :type => Integer, :default => nil,  :description => 'The port to connect to on the host, defaults to nil'
  cmd.define :duplicated_mapping,                   :default => nil,  :description => 'The mapping file to use when restoring an index, defaults to nil'
  cmd.define :batch_size,         :type => Integer, :default => nil,  :description => 'The number of lines to process at once, defaults to nil'
end
Settings.define_command  :cardinality, :description => "Count the number of unique valuse for the given field(s)" do |cmd|
  cmd.define :output_dir,                   :default => nil,  :description => 'Directory to put output, defaults to nil'
  cmd.define :cardinality, :type => Array,  :default => nil,  :description => 'Return the cardinality of the given fields, defaults to nil'
  cmd.define :card_file,                    :default => nil,  :description => 'The dump file to grab info from when determining cardinality MUST NOT be compressed, defaults to nil'
end
Settings.define_command :warmer, :description => "Interact elasticsearch warmers" do |cmd|
  cmd.define :warmers,                            :default => nil,  :description => 'Use warmers expected values true/false, defaults to nil'
  cmd.define :warmers_index,                      :default => nil,  :description => 'The index to add the warmer too, remove it from or disable/enable it on, defaults to nil'
  cmd.define :new_warmers_name,                   :default => nil,  :description => 'Name of warmer to create, defaults to nil'
  cmd.define :create_warmer,                      :default => nil,  :description => 'Query to create warmer, defaults to nil'
  cmd.define :remove_warmer,                      :default => nil,  :description => 'Name of warmer to remove, defaults to nil'
  cmd.define :host,                               :default => nil,  :description => 'The host to connect to, defaults to nil'
  cmd.define :port,             :type => Integer, :default => nil,  :description => 'The port to connect to on the host, defaults to nil'
end
Settings.define_command :cache, :description => "Interact with elasticsearch caches" do |cmd|
  cmd.define :host,                                   :default => nil,  :description => 'The host to connect to, defaults to nil'
  cmd.define :port,               :type => Integer,   :default => nil,  :description => 'The port to connect to on the host, defaults to nil'
  cmd.define :clear_all_cache,    :type => :boolean,  :default => nil,  :description => 'Clear all caches expected true/false, defaults to nil'
  cmd.define :clear_fielddata,    :type => :boolean,  :default => nil,  :description => 'Clear filter cache expected true/false, defaults to nil'
  cmd.define :clear_filter_cache, :type => :boolean,  :default => nil,  :description => 'Clear filter cache expected true/false, defaults to nil'
end
Settings.define_command :replay, :description => "Replay slow log queries" do |cmd|
  cmd.define :host,                                   :default => nil,  :description => 'The host to connect to, defaults to nil'
  cmd.define :port,                 :type => Integer, :default => nil,  :description => 'The port to connect to on the host, defaults to nil'
  cmd.define :execute_slow_queries,                   :default => nil,  :description => 'Execute the slow log queries in the provided log file,ie --execute_slow_log=/var/log/elasticsearch/padraig.log, defaults to nil'
  cmd.define :batch_size,           :type => Integer, :default => nil,  :description => 'The number of lines to process at once, defaults to nil'
end
Settings.define_command :index_settings, :description => "Change the index settings" do |cmd|
  cmd.define :host,                                       :default => nil,  :description => 'The host to connect to, defaults to nil'
  cmd.define :port,                     :type => Integer, :default => nil,  :description => 'The port to connect to on the host, defaults to nil'
  cmd.define :settings_index,                             :default => nil,  :description => 'The index that the settings listed in index_settings will be changed for, defaults to nil'
  cmd.define :es_index_settings,        :type => Array,   :default => nil,  :description => 'A comma deliminated list of elasticsearch index settings to be set for --settings_index, defaults to []'
  cmd.define :es_index_settings_values, :type => Array,   :default => nil,  :description => 'A comma deliminated list of elasticsearch index settings values to be set for --settings_index, defaults to []'
end
Settings.resolve!


Squirrel.new(Settings.command_name, Settings.to_hash).task_caller


