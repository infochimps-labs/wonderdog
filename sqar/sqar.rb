#### This is the uber script the arguements you give it decide what happens
## squirrel => Standard Query Ultracrepidate Iamatology Ruby Resource for Elasticsearch Labarum ##


require "configliere"
require_relative "../test/esbackup_stripped.rb"
require_relative "../test/cardinality.rb"
require_relative "../sqar/replay.rb"
require_relative "../test/warmer_interface.rb"

Settings.use :commandline
Settings.use :config_block
Settings.define :output_dir,            default: '',            description: 'Directory to put output, defaults to current path'
Settings.define :dump_file,             default: nil,           description: 'The name of the dumpfile to use, default is nil'
Settings.define :dump_index,            default: nil,           description: 'Create dump of the given index, default is nil'
Settings.define :query,                 default: nil,           description: 'Query to use in order to limit the data extracted from the index, default nil'
Settings.define :restore_index,         default: nil,           description: 'Restore the given index from --dump_file, default is nil'
Settings.define :create_index,          default: nil,           description: 'Create an index of the given name, default is nil'
Settings.define :duplicate_index,       default: nil,           description: 'Duplicate the given index, defaults to nil'
Settings.define :restore_index,         default: nil,           description: 'Restore the given index, defaults to nil'
Settings.define :cardinality,           default: nil,           description: 'Return the cardinality of the given index, defaults to nil'
Settings.define :cache,                 default: true,          description: 'Use cache, defaults to true'
Settings.define :temperature,           default: 'cold',        description: 'Hot or Cold cache, cold cache will clear cache before running queries, defaults to cold'
Settings.define :warmers,               default: false,         description: 'Use warmers, defaults to false'
Settings.define :new_warmers_name,      default: nil,           description: 'Name of warmer to create, defaults to nil'
Settings.define :create_warmer,         default: nil,           description: 'Query to create warmer, defaults to nil'
Settings.define :remove_warmer,         default: nil,           description: 'Name of warmer to remove, defaults to nil'
Settings.define :refresh_interval,      default: -1,            description: 'Set indexes cache refresh level, defaults to -1'
Settings.define :execute_slow_queries,  default: nil,           description: 'Execute the slow log queries in the given log file, defaults to nil'
Settings.define :batch_size,            default: 100,           description: 'Batch size when processing gzip file, defaults to 100'
Settings.define :dump_mapping,          default: nil,           description: 'The name of the file in which to dump the indexes mapping, defaults to nil'
Settings.define :restore_mapping,       defualt: nil,           description: 'The mapping json file to use when restoring the index, defaults to nil'
Settings.define :duplicate_mapping,     default: nil,           description: 'The mapping json file to use when duplicating documents in an index, defaults to nil'
Settings.define :host,                  default: nil,           description: 'The elasticsearch hostname, defaults to nil'
Settings.define :port,                  default: 9200,          description: 'The port on the elasticsearch host to connect to, defaults to 9200'
Settings.define :logfile,               default: nil,           description: 'Logfile to use for slow logs, default nil'
Settings.define :clear_all_cache,       default: false,         description: 'Clear all caches, defaults to false'
Settings.define :clear_filter_cache,    default: false,         description: 'Clear filter cache, defaults to false'
Settings.define :clear_fielddata,       default: false,         description: 'Clear fielddata, defaults to false'
Settings.resolve!


class SQAR

  def initialize(options = {})
    ##The next two lines are necessary if you want to run without configliere, as they enforce the non-nil defaults
    #defaults = {:output_dir => '', :cache => true, :warmers => false, :temperature => 'cold', :refresh_interval => -1,
    # :batch_size => 100, :port => 9200, "clear_all_cache" => false, "clear_filter_cache" => false,
    # "clear_fielddata" => false}
    #options = defaults.merge(options)

    @output_dir = options[:output_dir]

    @dump_file = options[:dump_file]
    @dump_index = options[:dump_index]
    @restore_index = options[:restore_index]
    @create_index = options[:create_index]
    @duplicate_index = options[:duplicate_index]

    @restore_index = options[:restore_index]
    @cardinality = options[:cardinality]
    @cache = options[:cache]
    @temperature = options[:temperature]
    @warmers = options[:warmers]

    @new_warmers_name = options[:new_warmers_name]
    @create_warmer = options[:create_warmer]
    @remove_warmer = options[:remove_warmer]
    @refresh_interval = options[:refresh_interval]
    @execute_slow_queries = options[:execute_slow_queries]

    @batch_size = options[:batch_size]
    @dump_mapping = options[:dump_mapping]
    @restore_mapping = options[:restore_mapping]
    @duplicate_mapping = options[:duplicate_mapping]
    @host = options[:host]

    @port = options[:port]
    @logfile = options[:logfile]
    @clear_all_cache = options[:clear_all_cache]
    @clear_filter_cache = options[:clear_filter_cache]
    @clear_fielddata = options[:clear_fielddata]
  end

  def is_not_nil?(param)
    !param.nil?
  end

  def is_bool?(param)
    if !!param = param
      return true
    else
      return false
    end
  end

  def build_task_controllers
    @some_option_names = %w[dump_index dump_mapping restore_index restore_mapping create_index duplicate_index
        duplicate_mapping restore_index cardinality new_warmers_name remove_warmer warmers create_warmer
        execute_slow_query logfile temperature clear_all_cache clear_filter_cache clear_fielddata cache refresh_interval]
    #puts "\n"
    #puts @some_option_names.inspect
    @tasks = %w[backup backup restore restore restore restore duplicate duplicate cardinality warmer warmer warmer
                replay replay cache cache cache cache index_settings index_settings refresh_interval]
    #puts @tasks.inspect
    @base_tasks_params = {:output_dir => @output_dir, :batch_size => @batch_size, :port => @port, :host => @host}
    #puts [@dump_index, @dump_mapping, @restore_index, @restore_mapping, @create_index, @duplicate_index,
    #      @duplicate_mapping, @restore_index, @cardinality, @new_warmers_name, @remove_warmer, @warmers,
    #      @create_warmer, @execute_slow_queries, @logfile, @temperature, @clear_all_cache,
          #@clear_fielddata, @clear_filter_cache, @cache, @refresh_interval].inspect
    @task_controllers = [@dump_index, @dump_mapping, @restore_index, @restore_mapping, @create_index, @duplicate_index,
                         @duplicate_mapping, @restore_index, @cardinality, @new_warmers_name, @remove_warmer, @warmers,
                         @create_warmer, @execute_slow_queries, @logfile, @temperature, @clear_all_cache,
                         @clear_fielddata, @clear_filter_cache, @cache, @refresh_interval].zip(@some_option_names, @tasks)
    #puts "\n"
    #@task_controllers.each do |pairs|
    #  puts pairs.inspect
    #end
    @execute_tasks = {}
  end

  def add_task?(var, var_name, task_name)
    if is_not_nil?(var) || (is_bool?(var) && var)
      @execute_tasks[task_name] ||= {}
      @execute_tasks[task_name][var_name.to_sym] = var
      unless @execute_tasks[task_name].has_key?(:cache)
        @execute_tasks[task_name].merge!(@base_tasks_params)
      end
    end
  end


  def determine_tasks
    @task_controllers.each do |var, var_sym, task|
      add_task?(var, var_sym, task)
    end
    puts @execute_tasks.inspect
  end

  def determine_warmer_action(options)
    if is_not_nil?(options[:remove_warmer])
      options[:action] = "remove_warmer"
      options[:warmer_name] = options[:remove_warmer]
      WarmerInterface.new(options).remove_warmer
    else
      if options[:warmer]
        options[:action] = "enable_warmer"
        WarmerInterface.new(options).enable_warmer
      else
        options[:action] = "disable_warmer"
        WarmerInterface.new(options).disable_warmer
      end
      unless options[:new_warmers_name].nil?
        options[:action] = "add_warmer"
        options[:warmer_name] = options[:new_warmers_name]
        options[:query] = options[:create_warmer]
        WarmerInterface.new(options).add_warmer
      end
    end
  end

  def task_caller
    @execute_tasks.each do |task, options|
      case command = task.to_sym
        when :restore
          options[:index] = options[:restore_index]
          options[:mappings] = options[:restore_mapping]
          ESRestore.new(options[:output_dir], options).run
        when :backup
          options[:index] = options[:dump_index]
          options[:mappings] = options[:dump_mapping]
          ESBackup.new(options[:output_dir], options).run
        when :duplicate
          options[:index] = options[:duplicate_index]
          options[:mappings] = options[:duplicate_mapping]
          ESDup.new(options[:output_dir], options).run
        when :cardinality
          Cardinality.new(options[:dump_file])
        when :warmer
          determine_warmer_action(options)
        when :replay
          Replay.new(options[:logfile], options[:host], options[:port])
        when :cache
          puts "clearing the caches is not implemented from this method."
        when :index_settings
          puts "changing the index settings is not implemented from this method yet"
        else abort Settings.help("Must specify either backup, restore or duplicate.  Got <#{command}> UPDATE THIS LINE!")
      end
    end
  end

  def run
    build_task_controllers
    determine_tasks
    #task_caller
  end
end

SQAR.new(Settings.to_hash).run

