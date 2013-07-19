require "rspec/core/rake_task"

desc "Run RSpec unit tests"
RSpec::Core::RakeTask.new(:spec) do |t|
  t.pattern       = 'spec/*_spec.rb'
end

desc "desc"
task :solr_bench, [:comp_cnt,:thread_cnt,:servers] do |t, args|
  args.with_defaults(
    :comp_cnt => 10
  )
end


