module Wukong
  module Elasticsearch

    # A class that makes Ruby's Time class serialize the way
    # Elasticsearch expects.
    #
    # Elasticsearch's date parsing engine [expects to
    # receive](http://www.elasticsearch.org/guide/reference/mapping/date-format.html)
    # a date formatted according to the Java library
    # [Joda's](http://joda-time.sourceforge.net/)
    # [ISODateTimeFormat.dateOptionalTimeParser](http://joda-time.sourceforge.net/api-release/org/joda/time/format/ISODateTimeFormat.html#dateOptionalTimeParser())
    # class.
    #
    # This format looks like this: `2012-11-30T01:15:23`.
    #
    # @see http://www.elasticsearch.org/guide/reference/mapping/date-format.html The Elasticsearch guide's Date Format entry
    # @see http://joda-time.sourceforge.net/api-release/org/joda/time/format/ISODateTimeFormat.html#dateOptionalTimeParser() The Joda class's API documentation
    class Timestamp < Time

      # Parses the given `string` into a Timestamp instance.
      #
      # @param [String] string
      # @return [Timestamp]
      def self.receive string
        return if string.nil? || string.empty?
        begin
          t = Time.parse(string)
        rescue ArgumentError => e
          return
        end
        new(t.year, t.month, t.day, t.hour, t.min, t.sec, t.utc_offset)
      end

      # Formats the Timestamp according to 
      # 
      def to_wire(options={})
        utc.strftime("%Y-%m-%dT%H:%M:%S")
      end
    end
  end
end
