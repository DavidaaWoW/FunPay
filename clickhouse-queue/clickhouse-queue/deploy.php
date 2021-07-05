#!/usr/bin/env php
<?php
/**
 * Using: deploy.php TASK [NAME]
 * TASK: deploy, start, restart, stop, log
 * NAME: whitelabel name or missing for all
 */

namespace REES46\Core;

$repo_url = 'git@bitbucket.org:mkechinov/rees46_clickhouse_queue.git';
$branch = 'master';
$ssh_options = [
	'user' => 'rails',
	'port' => 21212,
];

/**
 * Список серверов для деплоя
 */
$servers = [
	'rees46' => [
		'servers'   => ['148.251.91.107'],
		'deploy_to' => '/home/rails/rees46_clickhouse_queue',
		'ssh'       => $ssh_options,
	],
	'personaclick' => [
		'servers'   => ['88.99.209.134'],
		'deploy_to' => '/home/rails/queue.personaclick.com',
		'ssh'       => $ssh_options,
	],
	'technodom' => [
		'servers'   => ['194.169.87.42'],
		'deploy_to' => '/home/rails/queue.r46.technodom.kz',
		'ssh'       => $ssh_options,
	],
	'kameleoon' => [
		'servers'   => ['95.216.10.183'],
		'deploy_to' => '/home/rails/queue.products.kameleoon.com',
		'ssh'       => $ssh_options,
	],
];

/**
 * ------------- DEPLOY PROCESS --------------->
 */
require __DIR__ . '/vendor/autoload.php';
if( !class_exists('Composer\Autoload\ClassLoader', false) ) {
	die('You need to set up the project dependencies using the following commands:' . PHP_EOL . 'curl -s http://getcomposer.org/installer | php' . PHP_EOL . 'php composer.phar install' . PHP_EOL);
}

//Запускаем деплой
$level = Logger::TYPE_WARN;
Deploy::$reload_cmd = 'bin/run restart -d -v ' . $level;
$deploy = new Deploy($repo_url, $branch, $servers, $argv);
$deploy->execute($level);
