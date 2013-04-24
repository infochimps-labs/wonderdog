#### This is the uber script the arguements you give it decide what happens

require "configliere"
require_relative "../test/esbackup_stripped.rb"

Settings.use :commandline
Settings.use :config_block
Settings.define :output_dir,            default: '',           description: 'Directory to put output, defaults to current path'
Settings.define :dump_file,             default: nil,          description: 'The name of the dumpfile to use, default is nil'
Settings.define :dump_index,            default: nil,          description: 'Create dump of the given index, default is nil'
Settings.define :restore_index,         default: nil,          description: 'Restore the given index from --dump_file, default is nil'
Settings.define :create_index,          default: nil,          description: 'Create an index of the given name, default is nil'
Settings.define :duplicate_index,       default: nil,          description: 'Duplicate the given index, defaults to nil'
Settings.define :restore_index,         default: nil,          description: 'Restore the given index, defaults to nil'
Settings.define :cardinality,           default: nil,          description: 'Return the cardinality of the given index, defaults to nil'
Settings.define :cache,                 default: true,         description: 'Use cache, defaults to true'
Settings.define :temperature,           default: 'cold',       description: 'Hot or Cold cache, cold cache will clear cache before running queries, defaults to cold'
Settings.define :warmers,               default: false,        description: 'Use warmers, defaults to false'
Settings.define :new_warmers_name,      default: nil,          description: 'Name of warmer to create, defaults to nil'
Settings.define :create_warmer,         default: nil,          description: 'Query to create warmer, defaults to nil'
Settings.define :remove_warmer,         default: nil,          description: 'Name of warmer to remove, defaults to nil'
Settings.define :refresh_interval,      default: -1,           description: 'Set indexes cache refresh level, defaults to -1'
Settings.define :execute_slow_queries,  default: nil,          description: 'Execute the slow log queries in the given log file, defaults to nil '
Settings.resolve!


class SQAR

  def initialize(options = {})
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
    @tasks = %w[backup restore restore duplicate cardinality add_warmer remove_warmer replay]
    @base_tasks_params = {"cache" => @cache, "temperature" => @temperature, "warmers" => @warmers,
                          "refresh_interval" => @refresh_interval, "output_dir" => @output_dir}
    @task_controllers = [@dump_index, @restore_index, @duplicate_index, @restore_index, @cardinality, @new_warmers_name,
                         @remove_warmer, @execute_slow_queries].zip(@tasks)
    @execute_tasks = {}
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

  def add_task?(var, task_name)
    if is_not_nil?(var) || (is_bool?(var) && var)
      @execute_tasks[task_name] ||= {}
      @execute_tasks[task_name][var.to_sym] = var
      @execute_tasks[task_name].merge!(@base_tasks_params)
    end
  end


  def determine_tasks()
    @task_controllers.each do |var, task|
      add_task?(var, task)
    end
  end

  def task_caller
    @execute_tasks.each do |task, options|
      case command = task.to_sym
        when :restore then ESRestore.new(options["output_dir"], options).run
        when :backup  then ESBackup.new(options["output_dir"], options).run
        when :duplicate then ESDup.new(options["output_dir"], options).run
        else abort Settings.help("Must specify either backup, restore or duplicate.  Got <#{command}> UPDATE THIS LINE!")
      end
    end

  end
end

sqar_obj = SQAR.new(Settings.to_hash)
sqar_obj.determine_tasks
sqar_obj.task_caller

