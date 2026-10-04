<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Double;

use Flow\Parquet\ParquetFile\Metadata;
use Flow\Parquet\ParquetFile\RowGroups;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFileReader;
use Generator;

/**
 * Answers from a schema, records close() and every readColumns() call.
 */
final class SpyParquetFileReader implements ParquetFileReader
{
    public int $closes = 0;

    /**
     * @var list<array{list<string>, int, ?int, ?int}>
     */
    public array $reads = [];

    /**
     * @param list<array<string, list<mixed>>> $chunks
     */
    public function __construct(
        private readonly Schema $schema,
        private readonly array $chunks = [],
    ) {}

    public function close(): void
    {
        $this->closes++;
    }

    public function metadata(): Metadata
    {
        return new Metadata($this->schema, new RowGroups([]), 0, 1, null);
    }

    public function readColumns(array $columns, int $batchSize, ?int $limit, ?int $offset): Generator
    {
        $this->reads[] = [$columns, $batchSize, $limit, $offset];

        yield from $this->chunks;
    }

    public function rowsNumber(): int
    {
        return 0;
    }

    public function schema(): Schema
    {
        return $this->schema;
    }

    public function totalByteSize(): int
    {
        return 0;
    }
}
