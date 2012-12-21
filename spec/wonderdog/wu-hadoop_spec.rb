require 'spec_helper'

describe 'wu-hadoop' do

  context "when wonderdog hasn't been required" do
    let(:script) { examples_dir('no_wonderdog.rb') }
    it "doesn't recognize Elasticsearch URIs" do
      command('wu-hadoop', script, '--input=es://foo/bar', '--output=/some/path', '--dry_run').should_not have_stdout('elasticsearch')
    end
  end

  context "when wonderdog hasn't been required" do
    let(:script) { examples_dir('wonderdog.rb') }
    it "recognizes Elasticsearch URIs" do
      command('wu-hadoop', script, '--input=es://foo/bar', '--output=/some/path', '--dry_run').should have_stdout('elasticsearch')
    end
  end
end
