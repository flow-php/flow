<?php declare(strict_types=1);

if (!($_ENV['FLOW_PHAR_APP'] ?? false)) {
    require __DIR__ . '/../vendor/autoload.php';
}

\ini_set('memory_limit', -1);
