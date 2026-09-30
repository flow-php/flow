<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;
use Flow\Parquet\ParquetFileReader;
use Flow\Parquet\Writer;

/**
 * @implements ParquetOpener<ParquetFileReader>
 */
final readonly class EngineParquetOpener implements ParquetOpener
{
    public function __construct(
        private ParquetEngine $engine,
        private Options $options,
    ) {}

    /**
     * @return ParquetFile<ParquetFileReader>
     */
    public function file(SourceStream $stream): ParquetFile
    {
        return new ParquetFile($stream, $this->options, $this->engine->openForRead($stream));
    }

    public function sink(
        DestinationStream $stream,
        ParquetSchema $schema,
        Compressions $compressions,
        Options $options,
    ): ParquetOpenSink {
        $writer = new Writer(compression: $compressions, options: $options, engine: $this->engine);
        $writer->openForStream($stream, $schema);

        return new EngineParquetOpenSink($writer, new ParquetEncoder($schema));
    }

    /**
     * @param ParquetSourceFile<ParquetFileReader> $file
     */
    public function source(ParquetSourceFile $file): ParquetOpenSource
    {
        return new EngineParquetOpenSource($file->file);
    }
}
