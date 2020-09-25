role :web, '89.218.52.45'
role :app, '89.218.52.45'

set :stage, :technodom
set :default_stage, :technodom

set :application, 'queue.r46.technodom.kz'
set :deploy_to, "/home/rails/#{fetch(:application)}"