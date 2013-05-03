require "multi_json"
require_relative "../squirrel/esbackup_stripped.rb"
require_relative "../squirrel/replay.rb"
require_relative "../squirrel/warmer_interface.rb"
require_relative "../squirrel/clear_es_caches.rb"
require_relative "../squirrel/change_es_index_settings.rb"

class Squirrel

  def initialize(command, options = {})
    @command = command
    @options = options
  end

  def determine_warmer_action(options = {})
    options[:index] = options[:warmers_index]
    unless options[:remove_warmer].nil?
      puts "removing warmer"
      options[:action] = "remove_warmer"
      options[:warmer_name] = options[:remove_warmer]
    else
      if options[:warmers]
        puts "enabling warmers"
        options[:action] = "enable_warmer"
      elsif options[:warmers] == false
        puts "disabling warmers"
        options[:action] = "disable_warmer"
      end
      unless options[:new_warmers_name].nil?
        puts "adding warmer"
        options[:action] = "add_warmer"
        options[:warmer_name] = options[:new_warmers_name]
        options[:query] = options[:create_warmer]
      end
    end
    WarmerInterface.new(options).determine_interaction
  end

  def determine_cache_clear(options = {})
    if options[:clear_all_cache]
      options[:type] = "all"
      ClearESCaches.new(options).run
    end
    if options[:clear_filter_cache]
      options[:type] = "filter"
      ClearESCaches.new(options).run
    end
    if options[:clear_fielddata]
      options[:type] = "fielddata"
      ClearESCaches.new(options).run
    end
  end

  def cardinality(options)
    options[:cardinality].each do |field|
      output = `ruby getFields.rb --dump=#{options[:card_file]} --field=#{field} >> #{field}.txt ;
        cat #{field}.txt |sort | uniq -c |sort -n | wc -l;`
      puts "The number of values in #{field} form file #{ooptions[:card_file]} is #{output}"
    end
  end

  def task_caller
    puts "Running #{@command}"
    case @command
      when :restore
        @options[:index] = @options[:restore_index]
        @options[:mappings] = @options[:restore_mapping]
        ESRestore.new(@options[:restore_file], @options).run
      when :backup
        @options[:index] = @options[:dump_index]
        @options[:mappings] = @options[:dump_mapping]
        ESBackup.new(@options[:output_dir], @options).run
      when :duplicate
        @options[:index] = @options[:duplicate_index]
        @options[:mappings] = @options[:duplicate_mapping]
        ESDup.new(@options[:duplicate_file], @options).run
      when :cardinality
        cardinality(@options)
      when :warmer
        determine_warmer_action(@options)
      when :replay
        Replay.new(@options[:execute_slow_queries], @options[:host], @options[:port], @options[:preference], @options[:routing]).run
      when :cache
        determine_cache_clear(@options)
      when :index_settings
        unless @options[:es_index_settings].nil? || @options[:es_index_settings_values].nil?
          @options[:settings_and_values] = @options[:es_index_settings].zip(@options[:es_index_settings_values])
          ChangeESIndexSettings.new(@options).run
        else
          puts "both --es_index_settings and --es_index_settings_values are required to change index settings"
        end
      else abort Settings.help("Must specify either backup, restore, duplicate, cardinality, warmer, replay, cache or index_settings.  Got <#{@command}> UPDATE THIS LINE!")
    end
  end
end
