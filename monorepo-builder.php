<?php

declare(strict_types=1);

use Symplify\MonorepoBuilder\Config\MBConfig;

return static function (MBConfig $config): void {
    $config->defaultBranch('1.x');
    $config->packageDirectories([
        __DIR__ . '/src/core',
        __DIR__ . '/src/cli',
        __DIR__ . '/src/adapter',
        __DIR__ . '/src/bridge',
        __DIR__ . '/src/lib',
    ]);
};
