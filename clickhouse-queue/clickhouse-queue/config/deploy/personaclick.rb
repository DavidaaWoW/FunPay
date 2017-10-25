role :web, '94.130.90.232'
role :app, '94.130.90.232'

set :stage, :personaclick
set :default_stage, :personaclick

set :application, 'queue.personaclick.com'
set :deploy_to, "/home/rails/#{fetch(:application)}"