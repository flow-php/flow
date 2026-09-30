<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;
use Flow\Parquet\ParquetFileReader;

/**
 * @template R of ParquetFileReader
 */
interface ParquetOpener
{
    /**
     * The file on $stream, opened with the reader its source() reads through.
     *
     * @return ParquetFile<R>
     */
    public function file(SourceStream $stream): ParquetFile;

    public function sink(
        DestinationStream $stream,
        ParquetSchema $schema,
        Compressions $compressions,
        Options $options,
    ): ParquetOpenSink;

    /**
     * @param ParquetSourceFile<R> $file
     */
    public function source(ParquetSourceFile $file): ParquetOpenSource;
}
