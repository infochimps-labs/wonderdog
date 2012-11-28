module Wukong
  module Elasticsearch
    module IntegrationHelper

      def root
        @root ||= Pathname.new(File.expand_path('../../..', __FILE__))
      end

      def lib_dir
        root.join('lib')
      end

      def bin_dir
        root.join('bin')
      end
      
      def integration_env
        {
          "RUBYLIB" => [lib_dir.to_s, ENV["RUBYLIB"]].compact.join(':')
        }
      end

      def integration_cwd
        root.to_s
      end

    end
  end
end

