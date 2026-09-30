<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\Engine\Arrow\OptionsConverter;
use Flow\Parquet\Engine\Arrow\SchemaConverter as ArrowSchemaConverter;
use Flow\Parquet\Engine\ArrowParquetEngine;
use Flow\Parquet\Engine\Native\NativeParquetFile;
use Flow\Parquet\Engine\NativeParquetFileReader;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;

/**
 * @implements ParquetOpener<NativeParquetFileReader>
 */
final readonly class NativeParquetOpener implements ParquetOpener
{
    public function __construct(
        private Options $options,
    ) {}

    /**
     * @return ParquetFile<NativeParquetFileReader>
     */
    public function file(SourceStream $stream): ParquetFile
    {
        return new ParquetFile(
            $stream,
            $this->options,
            new NativeParquetFileReader(new NativeParquetFile($stream), $this->options),
        );
    }

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

    /**
     * @param ParquetSourceFile<NativeParquetFileReader> $file
     */
    public function source(ParquetSourceFile $file): ParquetOpenSource
    {
        return new NativeParquetOpenSource($file->file->reader()->file());
    }
}
