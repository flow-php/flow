<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Mother;

use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\ParquetEngine;
use InvalidArgumentException;

final class ParquetEngineMother
{
    public static function create(string $class): ParquetEngine
    {
        return match ($class) {
            RustParquetEngine::class => new RustParquetEngine(),
            PhpParquetEngine::class => new PhpParquetEngine(),
            default => throw new InvalidArgumentException("Unknown Parquet engine {$class}"),
        };
    }
}
