<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\Filesystem\DestinationStream;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;
use Flow\Parquet\Writer;

final readonly class EngineParquetOpener implements ParquetOpener
{
    public function __construct(
        private ParquetEngine $engine,
    ) {}

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

    public function source(ParquetSourceFile $file): ParquetOpenSource
    {
        return new EngineParquetOpenSource($file->file);
    }
}
