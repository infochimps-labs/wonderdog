class ChangeESIndexSettings
  def initialize(options = {})
    @host = options[:host]
    @port = options[:port]
    @index = options[:index]
    @settings_and_values = options[:settings_and_values]
  end

  def change_setting(setting, value)
    puts "changing setting #{setting} to value #{value}"
    `curl -s -XPUT 'http://#{@host}:#{@port}/#{@index}/_settings?pretty=true' -d '{ "#{setting}":"#{value}" }'`
  end

  def run
    @settings_and_values.each do |setting, value|
      puts "setting: #{@setting} and value: #{@value}"
      change_setting(setting, value)
    end
  end
end