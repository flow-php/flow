<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\Filesystem\DestinationStream;
use Flow\Parquet\Engine\Arrow\OptionsConverter;
use Flow\Parquet\Engine\Arrow\SchemaConverter as ArrowSchemaConverter;
use Flow\Parquet\Engine\ArrowParquetEngine;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;

final readonly class NativeParquetOpener implements ParquetOpener
{
    public function sink(
        DestinationStream $stream,
        ParquetSchema $schema,
        Compressions $compressions,
        Options $options,
    ): ParquetOpenSink {
        return new NativeParquetOpenSink(
            new NativeParquetWriter(
                $stream,
                ArrowSchemaConverter::toExtension($schema),
                ArrowParquetEngine::mapCompression($compressions),
                OptionsConverter::toExtension($options),
            ),
        );
    }

    public function source(ParquetSourceFile $file): ParquetOpenSource
    {
        return new NativeParquetOpenSource($file->stream);
    }
}
