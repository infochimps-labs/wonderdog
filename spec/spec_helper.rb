require 'wonderdog'
require 'wukong/spec_helpers'
require_relative('support/integration_helper')
require_relative('support/driver_helper')


RSpec.configure do |config|

  config.before(:each) do
    @orig_reg = Wukong.registry.show
  end

  config.after(:each) do
    Wukong.registry.clear!
    Wukong.registry.merge!(@orig_reg)
  end
    
  include Wukong::SpecHelpers
  include Wukong::Elasticsearch::IntegrationHelper
  include Wukong::Elasticsearch::DriverHelper
end

