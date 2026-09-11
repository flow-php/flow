<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Extractor\SelfDescribingFile;
use Flow\ETL\Extractor\SourceFile;
use Flow\ETL\Schema;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\ParquetFile;

use function count;

final readonly class ParquetSourceFile implements SelfDescribingFile
{
    /**
     * @param list<string> $columns
     */
    public function __construct(
        public ParquetFile $file,
        private SourceStream $stream,
        private SourceFile $source,
        private SchemaConverter $converter,
        private array $columns,
    ) {}

    public function close(): void
    {
        $this->stream->close();
    }

    /**
     * Pruning belongs to the describer, not the fold: it is applied per file, before the merge.
     */
    public function schema(): Schema
    {
        $schema = $this->converter->toFlow($this->file->schema());

        return count($this->columns) ? $schema->keep(...$this->columns) : $schema;
    }

    public function source(): SourceFile
    {
        return $this->source;
    }
}
