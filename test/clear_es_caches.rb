class ClearESCaches
  def initialize(options={})
    @to_clear = options[:type]
    @host = options[:host]
    @port = options[:port]
  end

  def clear_all
    `curl -s -XPOST 'http://#{@host}:#{@port}/_all/_cache/clear?field_data=true&filter=true&bloom=true' ; echo`
  end

  def clear_filter_cache
    `curl -s -XPOST 'http://#{@host}:#{@port}/_all/_cache/clear?field_data=false&filter=true&bloom=true' ; echo`
  end

  def clear_fielddata
    `curl -s -XPOST 'http://#{@host}:#{@port}/_all/_cache/clear?field_data=true&filter=false&bloom=true' ; echo`
  end

  def run
    puts @to_clear
    case command = @to_clear.to_sym
      when :all then
        clear_filter_cache
        clear_fielddata
      when :filter then clear_filter_cache
      when :fielddata then clear_fielddata
      else abort "#{command} not recognized"
    end
  end

end