require 'wonderdog'
require 'wukong/spec_helpers'

RSpec.configure do |config|

  config.before(:each) do
    Wukong::Log.level = Log4r::OFF
    @orig_reg = Wukong.registry.show
  end

  config.after(:each) do
    Wukong.registry.clear!
    Wukong.registry.merge!(@orig_reg)
  end

  include Wukong::SpecHelpers
  
  def root
    @root ||= Pathname.new(File.expand_path('../..', __FILE__))
  end

  def hadoop_runner *args, &block
    runner(Wukong::Hadoop::HadoopRunner, 'wu-hadoop', *args) do
      stub(:execute_command!)
      instance_eval(&block) if block_given?
    end
  end
  
end
