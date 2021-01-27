role :web, '95.216.10.183'
role :app, '95.216.10.183'

set :stage, :kameleoon

set :application, 'queue.products.kameleoon.com'
set :deploy_to, "/home/rails/#{fetch(:application)}"