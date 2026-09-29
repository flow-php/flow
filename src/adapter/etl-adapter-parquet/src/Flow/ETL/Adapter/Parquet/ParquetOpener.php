<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\Filesystem\DestinationStream;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;

interface ParquetOpener
{
    public function sink(
        DestinationStream $stream,
        ParquetSchema $schema,
        Compressions $compressions,
        Options $options,
    ): ParquetOpenSink;

    public function source(ParquetSourceFile $file): ParquetOpenSource;
}
