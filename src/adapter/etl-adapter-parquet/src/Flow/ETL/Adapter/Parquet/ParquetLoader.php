<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline\Closure;
use Flow\ETL\Row\Schema;
use Flow\ETL\Rows;
use Flow\Parquet\Options;
use Flow\Parquet\Writer;

/**
 * @implements Loader<array{
 *   path: Path
 * }>
 */
final class ParquetLoader implements Closure, Loader, Loader\FileLoader
{
    private readonly SchemaConverter $converter;

    private ?Schema $inferredSchema = null;

    private ?Writer $writer = null;

    public function __construct(
        private readonly Path $path,
        private readonly Options $options,
        private readonly ?Schema $schema = null,
    ) {
        $this->converter = new SchemaConverter();

        if ($this->path->isPattern()) {
            throw new \InvalidArgumentException("ParquetLoader path can't be pattern, given: " . $this->path->path());
        }
    }

    public function __serialize() : array
    {
        return [
            'path' => $this->path,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->path = $data['path'];
    }

    public function closure(Rows $rows, FlowContext $context) : void
    {
        $this->writer($context)->close();
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
            $this->writer($context)->writeRow($row->toArray());
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

    private function writer(FlowContext $context) : Writer
    {
        if ($this->writer !== null) {
            return $this->writer;
        }

        $this->writer = new Writer(
            options: $this->options
        );
        $this->writer->openForStream(
            $context->streams()->open(
                $this->path,
                'parquet',
                Mode::WRITE_BINARY,
                $context->threadSafe()
            )->resource(),
            $this->converter->toParquet($this->schema())
        );

        return $this->writer;
    }
}
