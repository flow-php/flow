<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\IO\Extractor;

use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\BatchableExtractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileColumnsSource;
use Flow\ETL\IO\Extractor\File\FileExtractor;
use Flow\ETL\IO\Extractor\File\FileRead;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\File\FileSplitReader;
use Flow\ETL\IO\Extractor\File\MetadataColumnsExtractor;
use Flow\ETL\IO\Extractor\File\PartitionTypes;
use Flow\ETL\IO\Extractor\File\SourceFiles;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Extractor\SplittableExtractor;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition\StringDefinition;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Generator;

use function implode;
use function sprintf;

final class TextExtractor implements
    BatchableExtractor,
    Extractor,
    FileExtractor,
    MetadataColumnsExtractor,
    SplittableExtractor
{
    private ?Schema $schema = null;

    private BatchSize $batchSize;

    private FileColumnsSource $fileColumns;

    private readonly SourceFiles $files;

    public function __construct(
        private readonly Path $path,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        $filesystem->supports($path)
            || throw InvalidArgumentException::unsupportedPath($filesystem, $path, 'from_text');

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
     * @return Generator<int, CorruptedRecordException|Rows, Signal|null, void>
     */
    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        yield from $this->reader()->extract($backend, $criteria);
    }

    public function reader(): FileSplitReader
    {
        return new FileSplitReader(
            new FileRead($this->files, $this->fileColumns->columns(), $this->schema(), $this->batchSize()),
            new TextFileBatches(),
        );
    }

    public function schema(): Schema
    {
        return $this->fileColumns->columns()->declare($this->schema ?? new Schema(new StringDefinition('text')));
    }

    public function partitionSchema(): Schema
    {
        return $this->fileColumns->columns()->partitions($this->schema ?? new Schema());
    }

    public function partitionTypes(PartitionTypes $types): static
    {
        $this->fileColumns = $this->fileColumns->withPartitionTypes($types);

        return $this;
    }

    public function prunes(ScalarFunction $conjunct): bool
    {
        return false;
    }

    /**
     * One split per admitted file: line-delimited text is cut per file for now.
     *
     * @return non-empty-list<FileSplit>
     */
    public function splits(Criteria $criteria, int $targetSplitBytes): array
    {
        return $this->files->perFile($criteria->pathFilter);
    }

    public function refresh(): void
    {
        $this->files->refresh();
    }

    public function source(): Path
    {
        return $this->path;
    }

    public function statistics(): Statistics
    {
        return new Statistics(size: $this->files->bytes());
    }

    public function withBatchSize(int $batchSize): static
    {
        $this->batchSize = new BatchSize($batchSize);

        return $this;
    }

    public function withMetadataColumns(bool $addMetadataColumns): static
    {
        $this->fileColumns = $this->fileColumns->withMetadataColumns($addMetadataColumns);

        return $this;
    }

    /**
     * @throws InvalidArgumentException when $schema has no "text" column, the one the lines are read into
     */
    public function withSchema(Schema $schema): static
    {
        if ($schema->findDefinition('text') === null) {
            throw new InvalidArgumentException(sprintf('from_text() reads every line into the "text" column, the schema declares no such column: %s', implode(
                ', ',
                $schema->names(),
            )));
        }

        $this->schema = $schema;

        return $this;
    }
}
