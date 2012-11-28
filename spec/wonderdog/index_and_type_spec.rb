require 'spec_helper'

describe Wukong::Elasticsearch::IndexAndType do

  subject { Wukong::Elasticsearch::IndexAndType }

  let(:filesystem_path)      { '/some/path'                                        }
  let(:filesystem_paths)     { '/some/path,/some/other/path'                       }
  
  let(:hdfs_path)            { 'hdfs://some/hdfs/path'                             }
  let(:hdfs_paths)           { 'hdfs://some/hdfs/path,hdfs://some/other/hdfs/path' }
  
  let(:es_index_and_type)    { 'es://index/type'                                   }
  let(:es_indices_and_type)  { 'es://index1,index2/type'                           }
  let(:es_index_and_types)   { 'es://index/type1,type2'                            }
  let(:es_indices_and_types) { 'es://index1,index2/type1,type2'                    }

  fails  = %w[filesystem_path filesystem_paths hdfs_path hdfs_paths]
  passes = %w[es_index_and_type es_indices_and_type es_index_and_types es_indices_and_types]
  
  context 'recognizing possible es://index/type specifications' do
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

  context "parsing es://index/type specifications" do
    fails.each do |name|
      it "raises an error on a #{name}" do
        lambda { subject.new(self.send(name)) }.should raise_error(Wukong::Error, /not an elasticsearch.*index\/type/i)
      end
    end

    it "raises an error on a specification with too many parts" do
      lambda { subject.new('es://index/type/extra') }.should raise_error(Wukong::Error, /not an elasticsearch.*index\/type/i)
    end

    it "raises an error on a specification with too few parts" do
      lambda { subject.new('es://') }.should raise_error(Wukong::Error, /not an elasticsearch.*index\/type/i)
    end
    
    context "on an index and type" do
      subject { Wukong::Elasticsearch::IndexAndType.new(es_index_and_type) }
      its(:index) { should == 'index'}
      its(:type)  { should == 'type' }
    end
    context "on indices and a type" do
      subject { Wukong::Elasticsearch::IndexAndType.new(es_indices_and_type) }
      its(:index) { should == 'index1,index2'}
      its(:type)  { should == 'type'         }
    end
    context "on an index and types" do
      subject { Wukong::Elasticsearch::IndexAndType.new(es_index_and_types) }
      its(:index) { should == 'index'       }
      its(:type)  { should == 'type1,type2' }
    end
    context "on indices and types" do
      subject { Wukong::Elasticsearch::IndexAndType.new(es_indices_and_types) }
      its(:index) { should == 'index1,index2'}
      its(:type)  { should == 'type1,type2'  }
    end
    
    
  end
  
end
