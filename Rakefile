#!/usr/bin/env ruby

require 'rubygems'
require 'configliere' ; Configliere.use(:commandline, :env_var, :define)

WORK_DIR=File.expand_path(File.dirname(__FILE__))
Settings.define :src,         :default => "#{WORK_DIR}/src",                :description => "Java source dir"
Settings.define :target,      :default => "#{WORK_DIR}/build",              :description => "Build target, this is where compiled classes live"
Settings.define :jar_name,    :default => "wonderdog",                      :description => "Name of the target jar"
Settings.define :hadoop_home, :default => "/usr/lib/hadoop",                :description => "Path to hadoop installation",       :env_var => "HADOOP_HOME"
Settings.define :es_home,     :default => "/usr/local/share/elasticsearch", :description => "Path to elasticsearch installation",:env_var => "ES_HOME"
Settings.define :pig_home,    :default => "/usr/local/share/pig",           :description => "Path to pig installation",          :env_var => "PIG_HOME"
Settings.resolve!
options = Settings.dup

#
# Returns full classpath
#
def classpath options
  cp = ["."]
  Dir[
    "#{options.hadoop_home}/hadoop*.jar",
    "#{options.hadoop_home}/lib/*.jar",
    "#{options.pig_home}/pig*.jar",
    "#{options.pig_home}/lib/*.jar",
    "/etc/elasticsearch/elasticsearch.yml",
    "#{options.es_home}/plugins/*",
    "#{options.es_home}/lib/*.jar",
    "#{options.es_home}/lib/sigar/*.jar"
  ].each{|jar| cp << jar}
  cp.join(':')
end

def srcs options
  sources = Dir[
    "#{options.src}/**/*.java",
  ].inject([]){|sources, src| sources << src; sources}
  sources.join(' ')
end

jar_dir = File.join(options.target, options.jar_name)
directory jar_dir

task :compile => jar_dir do
  sh "javac -cp #{classpath(options)} -d #{jar_dir} #{srcs(options)}"
end

task :jar => :compile do
  sh "jar -cvf  #{jar_dir}.jar -C #{jar_dir} . "
end

task :default => [:jar]
