<?php

declare(strict_types=1);

namespace Flow\Parquet;

use Flow\Filesystem\SourceStream;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Metadata;
use Flow\Parquet\ParquetFile\Page\ColumnPageHeader;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader\ColumnChunkViewer;
use Generator;

use function array_keys;
use function array_map;
use function array_values;
use function count;

/**
 * @template-covariant R of ParquetFileReader
 */
final class ParquetFile
{
    public const string PARQUET_MAGIC_NUMBER = 'PAR1';

    private const int VALUES_BATCH_SIZE = 1024;

    private bool $closed = false;

    /**
     * @param R $reader opened on $stream
     */
    public function __construct(
        private readonly SourceStream $stream,
        private readonly Options $options,
        private readonly ParquetFileReader $reader,
    ) {}

    public function __destruct()
    {
        $this->close();
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;
        $this->reader->close();
    }

    public function metadata(): Metadata
    {
        return $this->reader->metadata();
    }

    /**
     * @return \Generator<ColumnPageHeader>
     */
    public function pageHeaders(): Generator
    {
        foreach ($this->schema()->columnsFlat() as $column) {
            foreach ($this->viewChunksPages($column) as $pageHeader) {
                yield $pageHeader;
            }
        }
    }

    /**
     * @return R
     */
    public function reader(): ParquetFileReader
    {
        return $this->reader;
    }

    public function schema(): Schema
    {
        return $this->reader->schema();
    }

    /**
     * @param array<string> $columns
     *
     * @return Generator<int, array<string, list<mixed>>>
     */
    public function columns(int $batchSize, array $columns = [], ?int $limit = null, ?int $offset = null): Generator
    {
        if ($batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be greater than 0');
        }

        if ($limit !== null && $limit <= 0) {
            throw new InvalidArgumentException('Limit must be greater than 0');
        }

        if ($offset !== null && $offset < 0) {
            throw new InvalidArgumentException('Offset must be greater than or equal to 0');
        }

        if (!count($columns)) {
            $columns = array_map(static fn(Column $c) => $c->name(), $this->schema()->columns());
        }

        foreach ($columns as $columnName) {
            if (!$this->schema()->has($columnName)) {
                throw new InvalidArgumentException("Column \"{$columnName}\" does not exist");
            }
        }

        yield from $this->reader->readColumns(array_values($columns), $batchSize, $limit, $offset);
    }

    /**
     * @param array<string> $columns
     *
     * @return Generator<int, array<string, mixed>>
     */
    public function values(array $columns = [], ?int $limit = null, ?int $offset = null): Generator
    {
        foreach ($this->columns(self::VALUES_BATCH_SIZE, $columns, $limit, $offset) as $chunk) {
            $names = array_keys($chunk);
            $count = count($chunk[$names[0]]);

            for ($i = 0; $i < $count; $i++) {
                $row = [];

                foreach ($names as $name) {
                    $row[$name] = $chunk[$name][$i];
                }

                yield $row;
            }
        }
    }

    /**
     * @return \Generator<ColumnPageHeader>
     */
    private function viewChunksPages(FlatColumn $column): Generator
    {
        $viewer = new ColumnChunkViewer($this->options);

        foreach ($this->metadata()->rowGroups()->all() as $rowGroup) {
            foreach ($rowGroup->columnChunks() as $columnChunk) {
                foreach ($viewer->view($columnChunk, $this->stream) as $pageHeader) {
                    yield new ColumnPageHeader($column, $columnChunk, $pageHeader);
                }
            }
        }
    }
}
