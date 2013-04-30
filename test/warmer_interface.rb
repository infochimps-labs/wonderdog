require 'multi_json'
require 'httparty'

class WarmerInterface
  def initialize(options = {})
    @host = options[:host]
    @port = options[:port]
    @query = MultiJson.dump(options[:query])
    @warmer_name = options[:warmer_name]
    @index = options[:index]
    @action = options[:action]
    @warmer_state = nil
  end

  def add_warmer
    url = "http://#{@host}:#{@port}/#{@index}/_warmer/#{@warmer_name}"
    puts url
    puts @query
    response = HTTParty.put(url, {:body => @query})
    #puts "`curl -s -XPUT '#{@host}:#{@port}/#{@index}/_warmer/#{@warmer_name}' -d '#{@query}'`"
    #puts "\n"
    puts response
    #output = %x(curl -s -XPUT '#{@host}:#{@port}/#{@index}/_warmer/#{@warmer_name}' -d '#{@query}')
    #puts output
  end

  def remove_warmer
    response = `curl -s -XDELETE #{@host}:#{@port}/#{@index}/_warmer/#{@warmer_name}`
    puts response
  end

  def enable_warmer
    response = `curl -s -XPOST '#{@host}:#{@port}/#{@index}/_close'`
    puts response
    response = `curl -s -XPUT '#{@host}:#{@port}/#{@index}/_settings?pretty=true' -d '{"index.warmer.enabled":"true"}'`
    puts response
    response = `curl -s -XPOST '#{@host}:#{@port}/#{@index}/_open'`
    puts response
  end

  def disable_warmer
      response = `curl -s -XPOST '#{@host}:#{@port}/#{@index}/_close'`
      puts response
      response = `curl -s -XPUT '#{@host}:#{@port}/#{@index}/_settings?pretty=true' -d '{"index.warmer.enabled":"false"}'`
      puts response
      response =`curl -s -XPOST '#{@host}:#{@port}/#{@index}/_open'`
      puts response
  end

  def determine_interaction
    puts @index.inspect
    unless @index.nil? && @host.nil? && @port.nil?
      case command = @action.to_sym
        when :add_warmer then add_warmer
        when :remove_warmer then remove_warmer
        when :enable_warmer then enable_warmer
        when :disable_warner then disable_warmer
        else abort "#{command} is not a recognized action for #{self.name}"
      end
    end
  end
end
