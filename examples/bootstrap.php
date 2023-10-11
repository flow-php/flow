<?php declare(strict_types=1);

if (!($_ENV['FLOW_PHAR_APP'] ?? false)) {
    require __DIR__ . '/../vendor/autoload.php';
}

\ini_set('memory_limit', -1);

if (!\defined('__FLOW_DATA__')) {
    \define('__FLOW_DATA__', __DIR__ . '/data');
    \define('__FLOW_OUTPUT__', __DIR__ . '/output');
    \define('__FLOW_VAR__', __DIR__ . '/var');
    \define('__FLOW_VAR_RUN__', __DIR__ . '/var/run');
    \define('__FLOW_SRC__', __DIR__ . '/../src');
}

if (!\is_dir(__FLOW_VAR__)) {
    \mkdir(__FLOW_VAR__);
}

if (!\is_dir(__FLOW_VAR_RUN__)) {
    \mkdir(__FLOW_VAR_RUN__);
}
