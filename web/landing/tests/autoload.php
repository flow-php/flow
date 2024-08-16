<?php

declare(strict_types=1);

use Symfony\Component\Dotenv\Dotenv;

require_once __DIR__ . '/../vendor/autoload.php';

(new Dotenv())->loadEnv(__DIR__ . '/../.env.test');

if (\file_exists(__DIR__ . '/../var/log/')) {
    $testLogs = glob(__DIR__ . '/../var/log/*test.log');

    foreach ($testLogs as $testLog) {
        if (\is_file($testLog)) {
            \unlink($testLog);
        }
    }
}
