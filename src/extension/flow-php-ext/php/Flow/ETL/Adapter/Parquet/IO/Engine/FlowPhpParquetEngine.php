<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\IO\Engine;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

/**
 * @type FlowPhpEngineConfiguration = array{options: ParquetOptions}
 * @import-type ParquetOptions from Options
 *
 */
final class FlowPhpParquetEngine implements ParquetEngine
{
    public function __construct(?Options $options = null)
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public static function fromArray(array $configuration): static
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @return FlowPhpEngineConfiguration
     */
    public function normalize(): array
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function openForRead(SourceStream $stream): FlowPhpParquetFileReader
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @throws \Flow\Parquet\Exception\RuntimeException for Compressions::LZO
     */
    public function openForWrite(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
    ): FlowPhpParquetFileWriter {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @param iterable<array<array-key, mixed>> $rows
     */
    public function writeRows(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
        iterable $rows,
    ): void {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}
