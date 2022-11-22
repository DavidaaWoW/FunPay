#!/usr/bin/env php
<?php
/**
 * Основной файл запуска одного процесса сервера
 */

use REES46\ClickHouse\Processor;

define('APP_ROOT', realpath(__DIR__ . '/..'));
require APP_ROOT . '/vendor/rees46/core/bin/init.php';

$cli = WorkerRun::init();

$api = new Processor($cli);
$api->onWorkerStarted();