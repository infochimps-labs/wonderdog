require 'spec_helper'

describe Wukong::Elasticsearch::HadoopInvocationOverride do

  let(:no_es)      { driver('regexp',  'count', input: '/tmp/input_file',        output: '/tmp/output_file')         }
  let(:es_reader)  { driver('regexp',  'count', input: 'es://the_index/the_map', output: '/tmp/output_file')         }
  let(:es_writer)  { driver('regexp',  'count', input: '/tmp/input_file',        output: 'es:///the_index/the_map')  }
  let(:es_complex) { driver('regexp',  'count', input: 'es://the_index/the_map', output: 'es:///the_index/the_map', es_query: '{"hi": "there"}', es_request_size: 1000, es_index_field: 'ID') }

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
