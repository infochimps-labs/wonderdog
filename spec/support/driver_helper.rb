module Wukong
  module Elasticsearch
    module DriverHelper

      def driver *args
        params   = ::Wukong::Hadoop.configure(Configliere::Param.new)
        params.resolve!
        params.merge!(args.pop) if args.last.is_a?(Hash)
        Wukong::Hadoop::Driver.new(params, *args)
      end
      
    end
  end
end

