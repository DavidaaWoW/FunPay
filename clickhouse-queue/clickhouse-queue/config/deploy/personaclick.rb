role :web, '88.99.209.134'
role :app, '88.99.209.134'

set :stage, :personaclick
set :default_stage, :personaclick

set :application, 'queue.personaclick.com'
set :deploy_to, "/home/rails/#{fetch(:application)}"