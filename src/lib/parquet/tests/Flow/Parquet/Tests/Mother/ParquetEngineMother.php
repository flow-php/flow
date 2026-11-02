<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Mother;

use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use InvalidArgumentException;

final class ParquetEngineMother
{
    public static function create(string $class, Options $options = new Options()): ParquetEngine
    {
        return match ($class) {
            RustParquetEngine::class => new RustParquetEngine($options),
            PhpParquetEngine::class => new PhpParquetEngine(options: $options),
            default => throw new InvalidArgumentException("Unknown Parquet engine {$class}"),
        };
    }
}
