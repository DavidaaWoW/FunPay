<?php
namespace REES46\Test;

use League\CLImate\CLImate;
use REES46\Core\Logger;
use REES46\Core\Postgres;
use REES46\Core\WhiteLabel;

define('APP_ROOT', realpath(__DIR__ . '/..'));
require_once APP_ROOT . '/vendor/autoload.php';
require_once APP_ROOT . '/vendor/rees46/core/test/BaseTest.php';

//Читаем конфиги
$config = parse_ini_file(APP_ROOT . '/config/test_secrets.ini', true);
define('CONFIG', $config);

$cli = new CLImate();
$cli->arguments->add([
	'daemon'    => [
		'prefix'       => 'd',
		'description'  => 'Run as daemon',
		'noValue'      => true,
		'defaultValue' => true,
	],
	'log'       => [
		'prefix'       => 'l',
		'longPrefix'   => 'log',
		'description'  => 'Log file',
		'defaultValue' => APP_ROOT . '/log/test.log',
	],
	'log_level' => [
		'prefix'       => 'v',
		'longPrefix'   => 'log_level',
		'description'  => 'Log level, available: ' . implode(', ', [Logger::TYPE_DEBUG, Logger::TYPE_INFO, Logger::TYPE_WARN, Logger::TYPE_ERROR]),
		'defaultValue' => Logger::TYPE_DEBUG,
	],
]);

//Удаляем фай логов
if( file_exists($cli->arguments->get('log')) ) {
	file_put_contents($cli->arguments->get('log'), '');
}

//Инициализируем логгер
$logger = new Logger($cli, 'Test');
$logger->logger();

//Инициализация вайтлейбла
WhiteLabel::init($config['default']);
