<?php

declare(strict_types=1);

use Symplify\MonorepoBuilder\ComposerJsonManipulator\ValueObject\ComposerJsonSection;
use Symplify\MonorepoBuilder\Config\MBConfig;

return static function (MBConfig $config): void {
    $config->defaultBranch('1.x');
    $config->packageDirectories([
        __DIR__ . '/src/core',
        __DIR__ . '/src/adapter',
        __DIR__ . '/src/lib',
        __DIR__ . '/src/web',
    ]);

    $config->dataToAppend([
        ComposerJsonSection::SCRIPTS => [
            "build" => [
                "@static:analyze",
                "@test",
                "@test:mutation"
            ],
            "test" => [
                "tools/phpunit/vendor/bin/phpunit"
            ],
            "test:mutation" => [
                "tools/infection/vendor/bin/infection -j2"
            ],
            "static:analyze" => [
                "tools/psalm/vendor/bin/psalm.phar --output-format=compact",
                "tools/phpstan/vendor/bin/phpstan analyze -c phpstan.neon",
                "tools/cs-fixer/vendor/bin/php-cs-fixer fix --dry-run"
            ],
            "cs:php:fix" => [
                "tools/cs-fixer/vendor/bin/php-cs-fixer fix"
            ],
            "post-install-cmd" => [
                "@tools:install"
            ],
            "post-update-cmd" => [
                "@tools:install"
            ],
            "tools:install" => [
                "composer install --working-dir=./tools/cs-fixer",
                "composer install --working-dir=./tools/infection",
                "composer install --working-dir=./tools/monorepo",
                "composer install --working-dir=./tools/phpstan",
                "composer install --working-dir=./tools/psalm",
                "composer install --working-dir=./tools/phpunit"
            ]
        ]
    ]);

    $config->dataToRemove([
        ComposerJsonSection::REQUIRE_DEV => [
            "phpunit/phpunit" => "*",
            "infection/infection" => "*",
            "friendsofphp/php-cs-fixer" => "*",
            "phpstan/phpstan" => "*",
            "vimeo/psalm" => "*",
        ]
    ]);
};
