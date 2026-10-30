<?php

declare(strict_types=1);

namespace Flow\Parquet;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema;

use function interface_exists;

if (interface_exists(ParquetEngine::class, false)) {
    return;
}

interface ParquetEngine
{
    /**
     * @param array<array-key, mixed> $configuration untrusted: decoded from another process, validated here
     *
     * @throws InvalidArgumentException on a configuration this class cannot rebuild from
     */
    public static function fromArray(array $configuration): ParquetEngine;

    /**
     * @return null|array<string, mixed> null when this instance holds live state and cannot be rebuilt
     */
    public function normalize(): ?array;

    /**
     * The file on $stream, its footer read by the returned reader, which owns the stream from here on.
     */
    public function openForRead(SourceStream $stream): ParquetFileReader;

    public function openForWrite(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
    ): ParquetFileWriter;

    /**
     * @param iterable<array<array-key, mixed>> $rows
     */
    public function writeRows(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
        iterable $rows,
    ): void;
}
