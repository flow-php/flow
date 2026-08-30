<?php

declare(strict_types=1);

use Flow\Bridge\Mago\Types\FlowTypesPlugin;
use Mago\Sdk\Extension;
use Mago\Sdk\Worker;

foreach ([__DIR__ . '/../vendor/autoload.php', __DIR__ . '/../../../autoload.php'] as $autoloader) {
    if (file_exists($autoloader)) {
        require_once $autoloader;

        break;
    }
}

(new Worker(new Extension(
    identifier: 'flow-php/mago-types-bridge',
    name: 'Flow PHP Types',
    version: '1.0.0',
    analyzerPlugins: [new FlowTypesPlugin()],
)))->run();
