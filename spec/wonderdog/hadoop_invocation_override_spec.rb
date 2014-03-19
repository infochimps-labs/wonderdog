require 'spec_helper'

describe Wukong::Elasticsearch::HadoopInvocationOverride do

  let(:no_es)      { hadoop_runner('regexp',  'count', input: '/tmp/input_file',        output: '/tmp/output_file')         }
  let(:es_reader)  { hadoop_runner('regexp',  'count', input: 'es://the_index/the_map', output: '/tmp/output_file')         }
  let(:es_writer)  { hadoop_runner('regexp',  'count', input: '/tmp/input_file',        output: 'es:///the_index/the_map')  }
  let(:es_complex) { hadoop_runner('regexp',  'count', input: 'es://the_index/the_map', output: 'es:///the_index/the_map', es_query: '{"hi": "there"}', es_request_size: 1000, es_index_field: 'ID', map_speculative: true, reduce_speculative: true) }

  context "passing necessary jars to Hadoop streaming" do
    before  { Dir.stub(:[]).and_return(["/lib/dir/elasticsearch.jar"], ["/lib/dir/wonderdog.jar"]) }
    context "when not given explicit jars" do
      context "and not interacting with Elasticsearch" do
        it "doesn't add jars" do
          no_es.hadoop_commandline.should_not match('-libjars')
        end
      end
      context "and reading from Elasticsearch" do
        it "adds default jars it finds on the local filesystem" do
          es_reader.hadoop_commandline.should match('-libjars.*elasticsearch')
        end
      end
      context "and writing to Elasticsearch" do
        it "adds default jars it finds on the local filesystem" do
          es_writer.hadoop_commandline.should match('-libjars.*elasticsearch')
        end
      end
      context "and reading and writing to Elasticsearch" do
        it "adds default jars it finds on the local filesystem" do
          es_complex.hadoop_commandline.should match('-libjars.*elasticsearch')
        end
      end
    end
  end

  context "setting speculative execution" do
    context "when not given speculative options" do
      context "and not interacting with Elasticsearch" do
        it "doesn't add any speculative options" do
          no_es.hadoop_commandline.should_not match('speculative')
        end
      end
      context "and reading from Elasticsearch" do
        it "disables speculative execution in the mapper" do
          es_reader.hadoop_commandline.should match(/-D mapred.map.tasks.speculative.execution.*false/)
        end
        it "disables speculative execution in the reducer" do
          es_reader.hadoop_commandline.should match(/-D mapred.reduce.tasks.speculative.execution.*false/)
        end
      end
      context "and reading from Elasticsearch" do
        it "disables speculative execution in the mapper" do
          es_writer.hadoop_commandline.should match(/-D mapred.map.tasks.speculative.execution.*false/)
        end
        it "disables speculative execution in the reducer" do
          es_writer.hadoop_commandline.should match(/-D mapred.reduce.tasks.speculative.execution.*false/)
        end
      end
    end
    context "when given speculative options" do
      it "does not change them" do
        es_complex.hadoop_commandline.should match(/-D mapred.map.tasks.speculative.execution.*true/)
        es_complex.hadoop_commandline.should match(/-D mapred.reduce.tasks.speculative.execution.*true/)
      end
    end
  end
  
  context "handling input and output paths, formats, and options when" do

    context "not interacting with Elasticsearch" do
      subject                  { no_es                                            }
      # input
      its(:input_paths)        { should == '/tmp/input_file'                      }
      its(:hadoop_commandline) { should     match(%r{-input.*/tmp/input_file}i)   }

      # output
      its(:output_path)        { should == '/tmp/output_file'                     }
      its(:hadoop_commandline) { should     match(%r{-output.*/tmp/output_file}i) }

      # no elasticsearch anything
      its(:hadoop_commandline) { should_not match(/elasticsearch/i)               }
    end

    context "reading from Elasticsearch" do
      subject                  { es_reader                                                          }
      
      # input
      its(:input_paths)        { should     match(%r{/user.*wukong.*the_index.*the_map})            }
      its(:hadoop_commandline) { should     match(/-inputformat.*elasticsearch/i)                   }
      its(:hadoop_commandline) { should     match(%r{-input.*/user.*wukong.*the_index.*the_map}i)   }
      its(:hadoop_commandline) { should     match(/-D\s+elasticsearch\.input\.index.*the_index/i)   }
      its(:hadoop_commandline) { should     match(/-D\s+elasticsearch\.input\.map.*the_map/i)       }
      
      # output
      its(:output_path)        { should == '/tmp/output_file'                                       }
      its(:hadoop_commandline) { should_not match(/-outputformat/i)                                 }
      its(:hadoop_commandline) { should     match(%r{-output.*/tmp/output_file}i)                   }
      its(:hadoop_commandline) { should_not match(/-D\s+elasticsearch\.output/i)                    }
    end

    context "writing to Elasticsearch" do
      subject                  { es_writer                                                          }

      # input
      its(:input_paths)        { should == '/tmp/input_file'                                        }
      its(:hadoop_commandline) { should_not match(/-inputformat/i)                                  }
      its(:hadoop_commandline) { should     match(%r{-input.*/tmp/input_file}i)                     }
      its(:hadoop_commandline) { should_not match(/-D\s+elasticsearch\.input/i)                     }

      # output
      its(:output_path)        { should     match(%r{/user.*wukong.*the_index.*the_map})            }
      its(:hadoop_commandline) { should     match(/-outputformat.*elasticsearch/i)                  }
      its(:hadoop_commandline) { should     match(%r{-output.*/user.*wukong.*the_index.*the_map}i)  }
      its(:hadoop_commandline) { should     match(/-D\s+elasticsearch\.output\.index.*the_index/i)  }
      its(:hadoop_commandline) { should     match(/-D\s+elasticsearch\.output\.map.*the_map/i)      }
    end

    context "reading and writing with many options" do
      subject                  { es_complex                                                         }

      # input
      its(:input_paths)        { should     match(%r{/user.*wukong.*the_index.*the_map})            }
      its(:hadoop_commandline) { should     match(/-inputformat.*elasticsearch/i)                   }
      its(:hadoop_commandline) { should     match(%r{-input.*/user.*wukong.*the_index.*the_map}i)   }
      its(:hadoop_commandline) { should     match(/-D\s+elasticsearch\.input\.index.*the_index/i)   }
      its(:hadoop_commandline) { should     match(/-D\s+elasticsearch\.input\.map.*the_map/i)       }

      # output
      its(:output_path)        { should     match(%r{/user.*wukong.*the_index.*the_map})            }
      its(:hadoop_commandline) { should     match(/-outputformat.*elasticsearch/i)                  }
      its(:hadoop_commandline) { should     match(%r{-output.*/user.*wukong.*the_index.*the_map}i)  }
      its(:hadoop_commandline) { should     match(/-D\s+elasticsearch\.output\.index.*the_index/i)  }
      its(:hadoop_commandline) { should     match(/-D\s+elasticsearch\.output\.map.*the_map/i)      }

      # options
      its(:hadoop_commandline) { should     match(/-D\s+elasticsearch\.input\.query.*hi.*there/i)   }
      its(:hadoop_commandline) { should     match(/-D\s+elasticsearch\.input\.request_size.*1000/i) }
      its(:hadoop_commandline) { should     match(/-D\s+elasticsearch\.output\.index\.field.*ID/i)  }
    end
  end
  
end
