<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine;

use Flow\Arrow\Parquet\ParquetFile;
use Flow\Arrow\Parquet\RowsWriter;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\Engine\Arrow\OptionsConverter;
use Flow\Parquet\Engine\Arrow\SchemaConverter;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFileReader;
use Flow\Parquet\ParquetFileWriter;

use function extension_loaded;

final class ArrowParquetEngine implements ParquetEngine
{
    public function __construct(
        private readonly Options $options = new Options(),
    ) {
        if (!extension_loaded('arrow')) {
            throw new RuntimeException('ArrowParquetEngine requires the arrow extension (flow-php/arrow-ext).');
        }
    }

    public static function mapCompression(Compressions $compression): string
    {
        return match ($compression) {
            Compressions::UNCOMPRESSED => 'UNCOMPRESSED',
            Compressions::SNAPPY => 'SNAPPY',
            Compressions::GZIP => 'GZIP',
            Compressions::BROTLI => 'BROTLI',
            Compressions::LZ4, Compressions::LZ4_RAW => 'LZ4_RAW',
            Compressions::ZSTD => 'ZSTD',
            Compressions::LZO => throw new RuntimeException('LZO compression is not supported by the Arrow engine'),
        };
    }

    public function openForRead(SourceStream $stream): ParquetFileReader
    {
        return new ArrowParquetFileReader(new ParquetFile($stream), $this->options);
    }

    public function openForWrite(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
    ): ParquetFileWriter {
        return new ArrowParquetFileWriter(
            new RowsWriter(
                $stream,
                SchemaConverter::toExtension($schema),
                self::mapCompression($compression),
                OptionsConverter::toExtension($options),
                $this->options->getInt(Option::ARROW_WRITE_BATCH_SIZE),
            ),
        );
    }

    public function writeRows(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
        iterable $rows,
    ): void {
        $file = $this->openForWrite($stream, $schema, $compression, $options);

        try {
            $file->writeBatch($rows);
        } finally {
            $file->close();
        }
    }
}
