role :web, '194.169.87.42'
role :app, '194.169.87.42'

set :stage, :technodom
set :default_stage, :technodom

set :application, 'queue.r46.technodom.kz'
set :deploy_to, "/home/rails/#{fetch(:application)}"