require 'spec_helper'

describe Wukong::Elasticsearch::IndexAndMapping do

  subject { Wukong::Elasticsearch::IndexAndMapping }

  let(:filesystem_path)      { '/some/path'                                        }
  let(:filesystem_paths)     { '/some/path,/some/other/path'                       }
  
  let(:hdfs_path)            { 'hdfs://some/hdfs/path'                             }
  let(:hdfs_paths)           { 'hdfs://some/hdfs/path,hdfs://some/other/hdfs/path' }
  
  let(:es_index_and_mapping)    { 'es://index/mapping'                                   }
  let(:es_indices_and_mapping)  { 'es://index1,index2/mapping'                           }
  let(:es_index_and_mappings)   { 'es://index/mapping1,mapping2'                         }
  let(:es_indices_and_mappings) { 'es://index1,index2/mapping1,mapping2'                 }

  fails  = %w[filesystem_path filesystem_paths hdfs_path hdfs_paths]
  passes = %w[es_index_and_mapping es_indices_and_mapping es_index_and_mappings es_indices_and_mappings]
  
  context 'recognizing possible es://index/mapping specifications' do
    fails.each do |name|
      it "doesn't recognize a #{name}" do
        subject.matches?(self.send(name)).should be_false
      end
    end
    passes.each do |name|
      it "recognizes a #{name}" do
        subject.matches?(self.send(name)).should be_true
      end
    end
  end

  context "parsing es://index/mapping specifications" do
    fails.each do |name|
      it "raises an error on a #{name}" do
        lambda { subject.new(self.send(name)) }.should raise_error(Wukong::Error, /not an elasticsearch.*index\/mapping/i)
      end
    end

    it "raises an error on a specification with too many parts" do
      lambda { subject.new('es://index/mapping/extra') }.should raise_error(Wukong::Error, /not an elasticsearch.*index\/mapping/i)
    end

    it "raises an error on a specification with too few parts" do
      lambda { subject.new('es://') }.should raise_error(Wukong::Error, /not an elasticsearch.*index\/mapping/i)
    end
    
    context "on an index and mapping" do
      subject { Wukong::Elasticsearch::IndexAndMapping.new(es_index_and_mapping) }
      its(:index)   { should == 'index'   }
      its(:mapping) { should == 'mapping' }
    end
    context "on indices and a mapping" do
      subject { Wukong::Elasticsearch::IndexAndMapping.new(es_indices_and_mapping) }
      its(:index)   { should == 'index1,index2'   }
      its(:mapping) { should == 'mapping'         }
    end
    context "on an index and mappings" do
      subject { Wukong::Elasticsearch::IndexAndMapping.new(es_index_and_mappings) }
      its(:index)   { should == 'index'             }
      its(:mapping) { should == 'mapping1,mapping2' }
    end
    context "on indices and mappings" do
      subject { Wukong::Elasticsearch::IndexAndMapping.new(es_indices_and_mappings) }
      its(:index)   { should == 'index1,index2'      }
      its(:mapping) { should == 'mapping1,mapping2'  }
    end
    
    
  end
  
end
