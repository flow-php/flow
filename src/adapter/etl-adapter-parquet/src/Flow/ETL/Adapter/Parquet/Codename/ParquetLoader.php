<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Codename;

use codename\parquet\helper\ParquetDataWriter;
use codename\parquet\ParquetOptions;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline\Closure;
use Flow\ETL\Row\Schema;
use Flow\ETL\Rows;

/**
 * @implements Loader<array{
 *   path: Path,
 *   rows_in_group: int
 * }>
 */
final class ParquetLoader implements Closure, Loader, Loader\FileLoader
{
    private array $buffer;

    private readonly SchemaConverter $converter;

    private ?Schema $inferredSchema = null;

    private ?ParquetDataWriter $writer = null;

    public function __construct(
        private readonly Path $path,
        private readonly int $rowsInGroup = 10000,
        private readonly ?Schema $schema = null,
        private readonly ?ParquetOptions $options = null
    ) {
        $this->converter = new SchemaConverter();
        $this->buffer = [];

        if ($this->path->isPattern()) {
            throw new \InvalidArgumentException("ParquetLoader path can't be pattern, given: " . $this->path->path());
        }
    }

    public function __serialize() : array
    {
        return [
            'path' => $this->path,
            'rows_in_group' => $this->rowsInGroup,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->path = $data['path'];
        $this->rowsInGroup = $data['rows_in_group'];
    }

    public function closure(Rows $rows, FlowContext $context) : void
    {
        if (\count($this->buffer)) {
            $this->writer($context)->putBatch($this->buffer);
            $this->writer($context)->finish();
            $buffer = [];
        }

        $context->streams()->close($this->path);
        $this->writer = null;
    }

    public function destination() : Path
    {
        return $this->path;
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if (\count($context->partitionEntries())) {
            throw new RuntimeException('Partitioning is not supported yet');
        }

        if ($this->schema === null) {
            $this->inferSchema($rows);
        }

        foreach ($rows as $row) {
            $this->buffer[] = $row->toArray();

            if (\count($this->buffer) >= $this->rowsInGroup) {
                $this->writer($context)->putBatch($this->buffer);
                $this->writer($context)->finish();
                $this->buffer = [];
            }
        }
    }

    private function inferSchema(Rows $rows) : void
    {
        if ($this->inferredSchema === null) {
            $this->inferredSchema = $rows->schema();
        } else {
            $this->inferredSchema = $this->inferredSchema->merge($rows->schema());
        }
    }

    /**
     * @psalm-suppress InvalidNullableReturnType
     * @psalm-suppress NullableReturnStatement
     */
    private function schema() : Schema
    {
        /** @phpstan-ignore-next-line  */
        return $this->schema ?? $this->inferredSchema;
    }

    private function writer(FlowContext $context) : ParquetDataWriter
    {
        if ($this->writer !== null) {
            return $this->writer;
        }

        $this->writer = new ParquetDataWriter(
            $context->streams()->open(
                $this->path,
                'parquet',
                Mode::WRITE,
                $context->threadSafe()
            )->resource(),
            $this->converter->toParquet($this->schema()),
            $this->options
        );

        return $this->writer;
    }
}
