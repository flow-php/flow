<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine;

use Flow\Arrow\Parquet\ColumnsReader;
use Flow\Arrow\Parquet\ParquetFile;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Metadata;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFileReader;
use Generator;

use function sprintf;
use function str_starts_with;

final class ArrowParquetFileReader implements ParquetFileReader
{
    private ?Metadata $metadata = null;

    private bool $open = true;

    private ?Schema $schema = null;

    public function __construct(
        private readonly ParquetFile $file,
        private readonly Options $options,
    ) {}

    public function close(): void
    {
        $this->open || throw new RuntimeException('Reader is not open');

        $this->open = false;
        $this->file->close();
    }

    public function file(): ParquetFile
    {
        $this->open || throw new RuntimeException('Reader is not open');

        return $this->file;
    }

    public function metadata(): Metadata
    {
        $this->open || throw new RuntimeException('Reader is not open');

        return $this->metadata ??= Metadata::fromThrift($this->file->thrift());
    }

    public function readColumns(array $columns, int $batchSize, ?int $limit, ?int $offset): Generator
    {
        $this->open || throw new RuntimeException('Reader is not open');

        if (!$this->options->getBool(Option::INT_96_AS_DATETIME)) {
            foreach ($columns as $column) {
                foreach ($this->schema()->columnsFlat() as $leaf) {
                    if (
                        $leaf->type() === PhysicalType::INT96
                        && ($leaf->flatPath() === $column || str_starts_with($leaf->flatPath(), $column . '.'))
                    ) {
                        throw new InvalidArgumentException(sprintf(
                            'Parquet column "%s" holds INT96, which arrow reads only as a datetime '
                            . '(Option::INT_96_AS_DATETIME = true); read the file with \Flow\Parquet\Reader::php()',
                            $column,
                        ));
                    }
                }
            }
        }

        $reader = new ColumnsReader($this->file, $columns, $batchSize, $offset, $limit);

        try {
            while (($chunk = $reader->next()) !== null) {
                yield $chunk;
            }
        } finally {
            $reader->close();
        }
    }

    public function rowsNumber(): int
    {
        $this->open || throw new RuntimeException('Reader is not open');

        return $this->file->rowsNumber();
    }

    public function schema(): Schema
    {
        $this->open || throw new RuntimeException('Reader is not open');

        return $this->metadata?->schema() ?? ($this->schema ??= Schema::fromThrift($this->file->schema()));
    }

    public function totalByteSize(): int
    {
        $this->open || throw new RuntimeException('Reader is not open');

        return $this->file->totalByteSize();
    }
}
