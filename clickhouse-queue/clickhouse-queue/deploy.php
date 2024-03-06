#!/usr/bin/env php
<?php
/**
 * Using: deploy.php TASK [NAME]
 * TASK: deploy, start, restart, stop, log
 * NAME: whitelabel name or missing for all
 */

namespace REES46\Core;

$repo_url = 'ssh://git@git.jetbrains.space/rees46/dev/clickhouse-queue.git';
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
		'servers'   => ['10.2.1.41'],
		'deploy_to' => '/home/rails/rees46_clickhouse_queue',
		'ssh'       => $ssh_options,
	],
	'shopify' => [
		'servers'   => ['94.130.22.95'],
		'deploy_to' => '/home/rails/rees46_clickhouse_queue',
		'ssh'       => $ssh_options,
	],
	'personaclick' => [
		'servers'   => ['88.99.217.82'],
		'deploy_to' => '/home/rails/queue.personaclick.com',
		'ssh'       => $ssh_options,
	],
	'technodom' => [
		'servers'   => ['172.22.254.2'],
		'deploy_to' => '/home/rails/queue.r46.technodom.kz',
		'ssh'       => $ssh_options,
	],
	'kameleoon' => [
		'servers'   => ['95.216.10.183'],
		'deploy_to' => '/home/rails/queue.products.kameleoon.com',
		'ssh'       => $ssh_options,
	],
	'halyk' => [
		'servers'   => ['10.204.11.161'],
		'deploy_to' => '/home/rails/rees46_clickhouse_queue',
		'ssh'       => $ssh_options,
	],
	'kari' => [
		'servers'   => ['10.2.3.24'],
		'deploy_to' => '/home/rails/rees46_clickhouse_queue',
		'ssh'       => array_merge($ssh_options, ['port' => 22]),
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
Deploy::$reload_cmd = 'bin/run restart -d';
$deploy = new Deploy($repo_url, $branch, $servers, $argv);
$deploy->execute(Logger::TYPE_ERROR);
