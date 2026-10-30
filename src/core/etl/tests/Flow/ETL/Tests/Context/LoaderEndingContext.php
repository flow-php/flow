<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\API\Branch;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\IO\Loader;
use Flow\ETL\Runtime\Config;
use Flow\ETL\Tests\Double\ThrowWhenRowMatches;
use Throwable;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;

final class LoaderEndingContext
{
    public static function failedRun(Loader|Branch $write, ?Config $config = null): void
    {
        try {
            data_frame($config)
                ->read(from_array([['id' => 1, 'v' => 'a'], ['id' => 2, 'v' => 'b']]))
                ->batchSize(1)
                ->with(new ThrowWhenRowMatches('id', 2, new RuntimeException('boom')))
                ->write($write)
                ->run();
        } catch (Throwable) {
            // the run is expected to fail; what matters is which ending the write was given
        }
    }

    public static function thrownByRun(Loader|Branch $write, ?Config $config = null): ?Throwable
    {
        try {
            data_frame($config)
                ->read(from_array([['id' => 1]]))
                ->write($write)
                ->run();
        } catch (Throwable $failure) {
            return $failure;
        }

        return null;
    }
}
