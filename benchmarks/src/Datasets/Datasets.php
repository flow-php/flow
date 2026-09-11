<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Datasets;

use RuntimeException;

use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function realpath;
use function str_contains;
use function var_export;

final class Datasets
{
    public static function orders(int $rows): OrdersDataset
    {
        return new OrdersDataset($rows);
    }

    public static function sellers(int $rows): SellersDataset
    {
        return new SellersDataset($rows);
    }

    public static function text(int $rows): TextDataset
    {
        return new TextDataset($rows);
    }

    public static function reset(): void
    {
        $filesystem = native_local_filesystem();

        if ($filesystem->status(path(Paths::var())) === null) {
            return;
        }

        $real = realpath(Paths::var());

        if ($real === false || !str_contains($real, '/benchmarks/var')) {
            throw new RuntimeException('Refusing to clear unexpected directory: ' . var_export($real, true));
        }

        $filesystem->rm(path($real));
    }
}
