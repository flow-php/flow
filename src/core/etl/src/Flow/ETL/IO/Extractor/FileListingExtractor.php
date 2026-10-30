<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\File\FileColumnsSource;
use Flow\ETL\IO\Extractor\File\FileExtractor;
use Flow\ETL\IO\Extractor\File\ListedFileRow;
use Flow\ETL\IO\Extractor\File\ListingScan;
use Flow\ETL\IO\Extractor\File\SourceFiles;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Generator;

final class FileListingExtractor implements BatchableExtractor, Extractor, FileExtractor
{
    private ?Schema $schema = null;

    private ?Statistics $statistics = null;

    private BatchSize $batchSize;

    private FileColumnsSource $fileColumns;

    private readonly SourceFiles $files;

    public function __construct(
        private readonly Path $path,
        private readonly ListedFileRow $row,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        $filesystem->supports($path) || throw InvalidArgumentException::unsupportedPath($filesystem, $path, 'files');

        $this->files = new SourceFiles($filesystem, $this->path);
        $this->fileColumns = new FileColumnsSource($this->files);
        $this->batchSize = new BatchSize();
    }

    /**
     * @return int<1, max>
     */
    public function batchSize(): int
    {
        return $this->batchSize->size;
    }

    public function isRepeatable(): bool
    {
        return true;
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        return (new ListingScan($this->files, $this->fileColumns->columns(), $this->row))->scan(
            $backend,
            $criteria,
            $this->schema(),
            $this->batchSize(),
        );
    }

    public function partitionSchema(): Schema
    {
        return $this->fileColumns->columns()->partitions($this->schema ?? new Schema());
    }

    public function prunes(ScalarFunction $conjunct): bool
    {
        return false;
    }

    public function refresh(): void
    {
        $this->files->refresh();
        $this->statistics = null;
    }

    public function source(): Path
    {
        return $this->path;
    }

    public function schema(): Schema
    {
        return $this->fileColumns->columns()->declare($this->schema ?? $this->row->schema());
    }

    public function statistics(): Statistics
    {
        if ($this->statistics === null) {
            $this->statistics = new Statistics(
                rows: Cardinality::exact($this->files->count()),
                size: $this->files->bytes(),
            );
        }

        return $this->statistics;
    }

    public function withBatchSize(int $batchSize): static
    {
        $this->batchSize = new BatchSize($batchSize);

        return $this;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
