<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Cardinality;
use Flow\ETL\Extractor\SelfDescribingFile;
use Flow\ETL\Extractor\SourceFile;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\Schema;
use Flow\Parquet\ParquetFile;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFileReader;

use function array_filter;
use function count;
use function in_array;

/**
 * @template R of ParquetFileReader
 */
final readonly class ParquetSourceFile implements SelfDescribingFile
{
    /**
     * @param ParquetFile<R> $file
     * @param list<string> $columns
     */
    public function __construct(
        /** @var ParquetFile<R> */
        public ParquetFile $file,
        private SourceFile $source,
        private SchemaConverter $converter,
        private array $columns,
    ) {}

    public function close(): void
    {
        $this->file->close();
    }

    /**
     * @param ParquetOpener<R> $opener
     */
    public function open(ParquetOpener $opener): ParquetOpenSource
    {
        return $opener->source($this);
    }

    /**
     * Pruning belongs to the describer, not the fold: it is applied per file, before the merge. Only the projected
     * columns are converted, so a column outside the projection never has to have a Flow type.
     */
    public function schema(): Schema
    {
        $schema = $this->file->schema();

        if (!count($this->columns)) {
            return $this->converter->toFlow($schema);
        }

        return $this->converter
            ->toFlow(ParquetSchema::with(...array_filter($schema->columns(), fn(Column $column): bool => in_array(
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
            rows: Cardinality::exact($this->file->reader()->rowsNumber()),
            size: Cardinality::exact($this->file->reader()->totalByteSize()),
        );
    }
}
