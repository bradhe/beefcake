require 'bundler'
Bundler::GemHelper.install_tasks

desc 'Run all the tests in the test directory'
task :test do
  require 'test/unit'

  Dir[File.expand_path('../test/**.rb', __FILE__)].each do |file|
    puts "Loading #{file}"
    require file
  end
end
