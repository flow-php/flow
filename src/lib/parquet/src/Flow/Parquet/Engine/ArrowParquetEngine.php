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

final class ArrowParquetEngine implements ParquetEngine
{
    private ?Writer $arrowWriter = null;

    /**
     * @var array<string, array<mixed>>
     */
    private array $batch = [];

    private int $batchSize = 0;

    /**
     * @var array<string>
     */
    private array $colNames = [];

    public function __construct(
        private readonly Options $options = new Options(),
    ) {
        if (!\extension_loaded('arrow')) {
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

    public function closeWrite(): void
    {
        if ($this->arrowWriter === null) {
            throw new RuntimeException('Writer is not open');
        }

        if ($this->batchSize > 0) {
            $this->arrowWriter->writeBatch($this->batch);
        }

        $this->arrowWriter->close();
        $this->arrowWriter = null;
        $this->batch = [];
        $this->batchSize = 0;
        $this->colNames = [];
    }

    public function openForWrite(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
    ): void {
        $adapter = new DestinationStreamAdapter($stream);
        $extensionSchema = SchemaConverter::toExtension($schema);
        $compressionStr = self::mapCompression($compression);
        $extensionOptions = OptionsConverter::toExtension($options);

        $this->arrowWriter = new Writer($adapter, $extensionSchema, $compressionStr, $extensionOptions);

        /** @var array<string> $colNames */
        $colNames = \array_column($extensionSchema, 'name');
        $this->colNames = $colNames;
        $this->resetBatch();
    }

    public function readValues(
        SourceStream $stream,
        Schema $schema,
        array $columns = [],
        ?int $limit = null,
        ?int $offset = null,
    ): \Generator {
        $adapter = new SourceStreamAdapter($stream);
        $extensionOptions = OptionsConverter::toExtension($this->options);
        $reader = new Reader($adapter, $extensionOptions);

        try {
            $skipped = 0;
            $yielded = 0;

            while (null !== ($batch = $reader->readRowGroup($columns ?: null))) {
                $colNames = \array_keys($batch);

                if ($colNames === []) {
                    continue;
                }

                $rowCount = \count($batch[$colNames[0]]);

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

    public function writeBatch(iterable $rows): void
    {
        foreach ($rows as $row) {
            $this->writeRow($row);
        }
    }

    public function writeRow(array $row): void
    {
        if ($this->arrowWriter === null) {
            throw new RuntimeException('Writer is not open');
        }

        foreach ($this->colNames as $name) {
            $this->batch[$name][] = $row[$name] ?? null;
        }

        $this->batchSize++;

        if ($this->batchSize >= $this->options->getInt(Option::ARROW_WRITE_BATCH_SIZE)) {
            $this->arrowWriter->writeBatch($this->batch);
            $this->resetBatch();
        }
    }

    public function writeRows(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
        iterable $rows,
    ): void {
        $this->openForWrite($stream, $schema, $compression, $options);

        try {
            $this->writeBatch($rows);
        } finally {
            $this->closeWrite();
        }
    }

    private function resetBatch(): void
    {
        $this->batch = [];

        foreach ($this->colNames as $name) {
            $this->batch[$name] = [];
        }

        $this->batchSize = 0;
    }
}
