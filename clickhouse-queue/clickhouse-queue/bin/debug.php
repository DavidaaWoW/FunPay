#!/usr/bin/env php
<?php
/**
 * Основной файл запуска одного процесса сервера
 */

use REES46\ClickHouse\Processor;

require_once 'init.php';

$api = new Processor($cli, $config);
$api->onWorkerStarted();