role :web, '148.251.91.107'
role :app, '148.251.91.107'

set :application, 'rees46_clickhouse_queue'

set :linked_files, ['config/secrets.ini']

set :linked_dirs, %w(log vendor)

set :log_level, :debug

set :repo_url, 'git@bitbucket.org:mkechinov/rees46_clickhouse_queue.git'
set :brancn, 'master'
set :scm, :git

set :deploy_to, "/home/rails/#{fetch(:application)}"
set :deploy_via,      :remote_cache
set :ssh_options,     { forward_agent: true }
set :use_sudo,        false
set :keep_releases, 5

set :ssh_options, {
  user: 'rails',
  forward_agent: true,
  port: 21212
}

set :rvm_type, :user
set :rvm_ruby_string, '2.3.1'

namespace :deploy do
  task :finalize_update do ; end

  task :start do
    on roles(:app), in: :sequence, wait: 5 do
      within release_path do
        execute "cd #{current_path}; ./run start -d"
      end
    end
  end

  task :stop do
    on roles(:web), in: :sequence, wait: 5 do
      execute "cd #{release_path}; ./run stop"
    end
  end

  # Когда deploy запускается в первые он не проходит до конца а падает на :restart
  # поэтому нужно сначала запустить deploy:start
  task :restart do
    on roles(:app), in: :sequence, wait: 5 do
      execute "cd #{release_path}; ./run restart -d"
    end
  end

  after :log_revision, :restart
end
