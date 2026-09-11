<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Datasets;

use RuntimeException;

use function getenv;

final class Paths
{
    public static function projectRoot(): string
    {
        $root = getenv('FLOW_MONOREPO_PROJECT_ROOT');

        if ($root === false || $root === '') {
            throw new RuntimeException('FLOW_MONOREPO_PROJECT_ROOT is not set; bootstrap.php must be loaded.');
        }

        return $root;
    }

    public static function root(): string
    {
        return self::projectRoot() . '/benchmarks';
    }

    public static function datasets(): string
    {
        return self::root() . '/datasets';
    }

    public static function var(): string
    {
        return self::root() . '/var';
    }
}
