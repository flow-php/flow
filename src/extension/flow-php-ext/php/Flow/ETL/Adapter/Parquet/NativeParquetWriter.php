<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Rows;
use Flow\Filesystem\DestinationStream;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

final class NativeParquetWriter
{
    /**
     * @param array<array-key, mixed> $schema Flow\Parquet\Engine\Arrow\SchemaConverter::toExtension()
     * @param array<string, mixed> $options Flow\Parquet\Engine\Arrow\OptionsConverter::toExtension()
     */
    public function __construct(DestinationStream $stream, array $schema, string $compression, array $options)
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function close(): void
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * The writer schema's columns by name; a column the rows lack is written as nulls.
     */
    public function write(Rows $rows): void
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}
