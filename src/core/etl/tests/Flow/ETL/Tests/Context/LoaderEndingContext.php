<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Config;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Loader;
use Flow\ETL\Tests\Double\ThrowWhenRowMatches;
use Throwable;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;

final class LoaderEndingContext
{
    public static function failedRun(Loader $loader, ?Config $config = null): void
    {
        try {
            data_frame($config)
                ->read(from_array([['id' => 1, 'v' => 'a'], ['id' => 2, 'v' => 'b']]))
                ->batchSize(1)
                ->with(new ThrowWhenRowMatches('id', 2, new RuntimeException('boom')))
                ->write($loader)
                ->run();
        } catch (Throwable) {
            // the run is expected to fail; what matters is which ending the sink was given
        }
    }

    public static function thrownByRun(Loader $loader, ?Config $config = null): ?Throwable
    {
        try {
            data_frame($config)
                ->read(from_array([['id' => 1]]))
                ->write($loader)
                ->run();
        } catch (Throwable $failure) {
            return $failure;
        }

        return null;
    }
}
