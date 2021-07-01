<?php

use League\CLImate\CLImate;
use REES46\Core\Logger;

putenv('AMP_LOG_COLOR=1');
define('APP_ROOT', realpath(__DIR__ . '/..'));
require APP_ROOT . '/vendor/autoload.php';
if( !class_exists('Composer\Autoload\ClassLoader', false) ) {
	die('You need to set up the project dependencies using the following commands:' . PHP_EOL . 'curl -s http://getcomposer.org/installer | php' . PHP_EOL . 'php composer.phar install' . PHP_EOL);
}

$cli = new CLImate();
$cli->arguments->add([
	'daemon'    => [
		'prefix'      => 'd',
		'description' => 'Run as daemon',
		'noValue'     => true,
	],
	'log'       => [
		'prefix'       => 'l',
		'longPrefix'   => 'log',
		'description'  => 'Log file',
		'defaultValue' => APP_ROOT . '/log/server.log',
	],
	'log_level' => [
		'prefix'       => 'v',
		'longPrefix'   => 'log_level',
		'description'  => 'Log level, available: ' . implode(', ', [Logger::TYPE_DEBUG, Logger::TYPE_INFO, Logger::TYPE_WARN, Logger::TYPE_ERROR]),
		'defaultValue' => Logger::TYPE_WARN,
	],
	'help'      => [
		'prefix'      => 'h',
		'longPrefix'  => 'help',
		'description' => 'Prints a usage statement',
		'noValue'     => true,
	],
]);
try {
	$cli->arguments->parse();
} catch (\League\CLImate\Exceptions\InvalidArgumentException $e) {
	exit($e->getMessage() . PHP_EOL);
}

//Читаем конфиги
$config = parse_ini_file(APP_ROOT . '/config/secrets.ini', true);
