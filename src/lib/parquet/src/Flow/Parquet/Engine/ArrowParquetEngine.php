<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine;

use Flow\Arrow\Parquet\Writer;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Engine\Arrow\DestinationStreamAdapter;
use Flow\Parquet\Engine\Arrow\OptionsConverter;
use Flow\Parquet\Engine\Arrow\SchemaConverter;
use Flow\Parquet\Engine\Native\NativeParquetFile;
use Flow\Parquet\Engine\Native\NativeParquetRowsWriter;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFileReader;
use Flow\Parquet\ParquetFileWriter;

use function array_column;
use function extension_loaded;
use function trigger_error;

use const E_USER_DEPRECATED;

final class ArrowParquetEngine implements ParquetEngine
{
    public function __construct(
        private readonly Options $options = new Options(),
    ) {
        if (!extension_loaded('flow_php') && !extension_loaded('arrow')) {
            throw new RuntimeException('ArrowParquetEngine requires the flow_php extension (flow-php/flow-php-ext).');
        }

        if (!extension_loaded('flow_php')) {
            @trigger_error(
                'Parquet through the arrow extension is deprecated and will be removed; install the flow_php '
                . 'extension (flow-php/flow-php-ext), which ArrowParquetEngine uses when it is loaded.',
                E_USER_DEPRECATED,
            );
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
        return extension_loaded('flow_php')
            ? new NativeParquetFileReader(new NativeParquetFile($stream), $this->options)
            : new ArrowParquetFileReader(
                new PhpParquetFileReader($stream, ByteOrder::LITTLE_ENDIAN, $this->options),
                $stream,
                $this->options,
            );
    }

    public function openForWrite(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
    ): ParquetFileWriter {
        $extensionSchema = SchemaConverter::toExtension($schema);

        if (extension_loaded('flow_php')) {
            return new NativeParquetFileWriter(
                new NativeParquetRowsWriter(
                    $stream,
                    $extensionSchema,
                    self::mapCompression($compression),
                    OptionsConverter::toExtension($options),
                    $this->options->getInt(Option::ARROW_WRITE_BATCH_SIZE),
                ),
            );
        }

        /** @var list<string> $columnNames */
        $columnNames = array_column($extensionSchema, 'name');
        // the arrow Writer takes the stream by reference, so it has to be a variable
        $adapter = new DestinationStreamAdapter($stream);

        return new ArrowParquetFileWriter(
            new Writer(
                $adapter,
                $extensionSchema,
                self::mapCompression($compression),
                OptionsConverter::toExtension($options),
            ),
            $stream,
            $columnNames,
            // write batching is an engine option, not a per-file one
            $this->options->getInt(Option::ARROW_WRITE_BATCH_SIZE),
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
