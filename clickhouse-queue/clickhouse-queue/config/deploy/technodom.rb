role :web, '109.166.71.13'
role :app, '109.166.71.13'

set :stage, :technodom
set :default_stage, :technodom

set :application, 'queue.r46.technodom.kz'
set :deploy_to, "/home/rails/#{fetch(:application)}"