<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine;

use Flow\Arrow\Parquet\Reader;
use Flow\Arrow\Parquet\Writer;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\Engine\Arrow\DestinationStreamAdapter;
use Flow\Parquet\Engine\Arrow\OptionsConverter;
use Flow\Parquet\Engine\Arrow\SchemaConverter;
use Flow\Parquet\Engine\Arrow\SourceStreamAdapter;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFileWriter;
use Generator;

use function array_column;
use function array_keys;
use function count;
use function extension_loaded;

final class ArrowParquetEngine implements ParquetEngine
{
    public function __construct(
        private readonly Options $options = new Options(),
    ) {
        if (!extension_loaded('arrow')) {
            throw new RuntimeException('The arrow extension is required for ArrowParquetEngine. '
            . 'Install it from flow-php/arrow-ext.');
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

    public function openForWrite(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
    ): ParquetFileWriter {
        $extensionSchema = SchemaConverter::toExtension($schema);

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

    /**
     * @param array<string> $columns
     *
     * @return \Generator<int, array<string, mixed>>
     */
    public function readValues(
        SourceStream $stream,
        Schema $schema,
        array $columns = [],
        ?int $limit = null,
        ?int $offset = null,
    ): Generator {
        $adapter = new SourceStreamAdapter($stream);
        $extensionOptions = OptionsConverter::toExtension($this->options);
        $reader = new Reader($adapter, $extensionOptions);

        try {
            $skipped = 0;
            $yielded = 0;

            while (null !== ($batch = $reader->readRowGroup($columns ?: null))) {
                $colNames = array_keys($batch);

                if ($colNames === []) {
                    continue;
                }

                $rowCount = count($batch[$colNames[0]]);

                for ($i = 0; $i < $rowCount; $i++) {
                    if ($offset !== null && $skipped < $offset) {
                        $skipped++;

                        continue;
                    }

                    if ($limit !== null && $yielded >= $limit) {
                        return;
                    }

                    $row = [];

                    foreach ($colNames as $name) {
                        $row[$name] = $batch[$name][$i];
                    }

                    yield $row;
                    $yielded++;
                }
            }
        } finally {
            $reader->close();
        }
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
