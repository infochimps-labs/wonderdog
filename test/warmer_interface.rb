require 'multi_json'

class WarmerInterface
  def initialize(options = {})
    @host = options[:host]
    @port = options[:port]
    @query = options[:query]
    @warmer_name = options[:warmer_name]
    @index = options[:index]
    @action = options[:action]
    @warmer_state = nil
  end

  def add_warmer
    `curl -XPUT #{@host}:#{@port}/#{@index}/_warmer/#{@warmer_name} -d '#{@query}'`
  end

  def remove_warmer
    `curl -XDELETE #{@host}:#{@port}/#{@index}/_warmer/#{@warmer_name}`
  end

  def get_warmer_state
    #TODO! Make sure this works!
    #Since index.warmer,enable does not defaultly exist must explicitly check true and false
    index_settings = `curl -XGET '#{@host}:#{@port}/#{@index}/_settings?pretty=true'`
    if index_settings.include?("\"index.warmer.enabled\" : \"false\",")
      @warmer_state = "off"
    elsif index_settings.include?("\"index.warmer.enabled\" : \"true\",")
      @warmer_state = "on"
    end
  end

  def enable_warmer
    #get_warmer_state
    #if @warmer_state == "off"
      `curl -XPOST '#{@host}:#{@port}/#{@index}/_close'`
      `curl -XPUT '#{@host}:#{@port}/#{@index}/_settings?pretty=true' -d '{"index.warmer.enabled":"true"}'`
      `curl -XPOST '#{@host}:#{@port}/#{@index}/_open'`
      #@warmer_state = "on"
    #end
  end

  def disable_warmer
    #get_warmer_state
    #if @warmer_state == "on"
      `curl -XPOST '#{@host}:#{@port}/#{@index}/_close'`
      `curl -XPUT '#{@host}:#{@port}/#{@index}/_settings?pretty=true' -d '{"index.warmer.enabled":"false"}'`
      `curl -XPOST '#{@host}:#{@port}/#{@index}/_open'`
      #@warmer_state = "off"
    #end
  end

  def determine_interaction
    case command = @action.to_sym
      when :add_warmer then add_warmer
      when :remove_warmer then remove_warmer
      when :enable_warmer then enable_warmer
      when :disable_warner then disable_warmer
      else abort "#{command} is not a recognized action for #{self.name}"
    end
  end
end
