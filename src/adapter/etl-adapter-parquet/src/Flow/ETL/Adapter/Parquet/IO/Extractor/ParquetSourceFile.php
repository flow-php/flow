<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\IO\Extractor;

use Flow\ETL\Adapter\Parquet\Values\Schema\SchemaConverter;
use Flow\ETL\Foundation\FlowPhpExtension;
use Flow\ETL\IO\Extractor\File\ByteUnit;
use Flow\ETL\IO\Extractor\File\SelfDescribingFile;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Schema;
use Flow\Parquet\Engine\RustParquetFileReader;
use Flow\Parquet\ParquetFile;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFileReader;

use function array_filter;
use function count;
use function in_array;
use function min;

final readonly class ParquetSourceFile implements SelfDescribingFile
{
    /**
     * @param ParquetFile<ParquetFileReader> $file
     * @param list<string> $columns
     */
    public function __construct(
        /** @var ParquetFile<ParquetFileReader> */
        public ParquetFile $file,
        private SourceFile $source,
        private SchemaConverter $converter,
        private array $columns,
        private ?FlowPhpExtension $extension = null,
    ) {}

    public function close(): void
    {
        $this->file->close();
    }

    /**
     * This file when it is $source, else it closes itself and answers null.
     */
    public function forSource(SourceFile $source): ?self
    {
        if ($this->source()->uri() === $source->uri()) {
            return $this;
        }

        $this->close();

        return null;
    }

    public function open(): ParquetOpenSource
    {
        $reader = $this->file->reader();

        return ($this->extension ?? FlowPhpExtension::detect())->available() && $reader instanceof RustParquetFileReader
            ? new RustParquetOpenSource($reader)
            : new PhpParquetOpenSource($this->file);
    }

    /**
     * Pruning belongs to the describer, not the fold: it is applied per file, before the merge. Only the projected
     * columns are converted, so a column outside the projection never has to have a Flow type.
     */
    public function schema(): Schema
    {
        $schema = $this->file->schema();

        if (!count($this->columns)) {
            return $this->converter->toFlowSchema($schema);
        }

        return $this->converter
            ->toFlowSchema(ParquetSchema::with(...array_filter($schema->columns(), fn(Column $column): bool => in_array(
                $column->name(),
                $this->columns,
                true,
            ))))
            ->keep(...$this->columns);
    }

    public function source(): SourceFile
    {
        return $this->source;
    }

    /**
     * Uncompressed row-group bytes, as the format reports them.
     */
    public function statistics(): Statistics
    {
        return new Statistics(
            rows: Cardinality::exact($this->file->reader()->footer()->rowsNumber()),
            size: Cardinality::exact($this->file->reader()->footer()->totalByteSize()),
        );
    }

    /**
     * One unit per row group: from its first page to the end of its compressed column chunks.
     */
    public function units(): array
    {
        $units = [];

        foreach ($this->file
            ->reader()
            ->footer()
            ->metadata()
            ->rowGroups()
            ->all() as $rowGroup) {
            $start = null;
            $bytes = 0;

            foreach ($rowGroup->columnChunks() as $columnChunk) {
                $start = $start === null ? $columnChunk->pageOffset() : min($start, $columnChunk->pageOffset());
                $bytes += $columnChunk->totalCompressedSize();
            }

            $units[] = new ByteUnit($start ?? 0, $bytes, $rowGroup->rowsCount());
        }

        return $units;
    }
}
