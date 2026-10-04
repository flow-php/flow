<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\BatchableExtractor;
use Flow\ETL\Extractor\Batches;
use Flow\ETL\Extractor\File\FileExtractor;
use Flow\ETL\Extractor\File\FileReading;
use Flow\ETL\Extractor\File\FileReadLoop;
use Flow\ETL\Extractor\File\ListedFiles;
use Flow\ETL\Extractor\File\MetadataColumnsExtractor;
use Flow\ETL\Extractor\File\ReadWindow;
use Flow\ETL\Extractor\RewindableExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Generator;

use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function implode;
use function iterator_to_array;
use function sprintf;

final class TextExtractor implements
    BatchableExtractor,
    Extractor,
    FileExtractor,
    MetadataColumnsExtractor,
    RewindableExtractor
{
    private ?Schema $schema = null;

    use Batches;
    use FileReading;

    private readonly Filesystem $filesystem;

    private ?ListedFiles $listed = null;

    public function __construct(
        private readonly Path $path,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        if (!$filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. from_text($path, filesystem: aws_s3_filesystem(...)).',
                $filesystem::class,
                $filesystem->mount()->protocol,
                $path->uri(),
            ));
        }

        $this->filesystem = $filesystem;
    }

    public function isRepeatable(): bool
    {
        return true;
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context, ?int $limit = null, Filter $pathFilter = new OnlyFiles()): Generator
    {
        $fileColumns = $this->fileColumns($this->filesystem, $this->path);

        yield from (new FileReadLoop(
            $fileColumns,
            $fileColumns->declare($this->schema ?? schema(str_schema('text'))),
        ))->read(
            iterator_to_array($this->sourceFiles($this->filesystem, $this->path, $pathFilter), false),
            new TextFileBatches($this->filesystem),
            $this->batchSize(),
            $context->backend(),
            new ReadWindow(limit: $limit),
        );
    }

    public function schema(): Schema
    {
        return $this->fileColumns($this->filesystem, $this->path)->declare($this->schema ?? schema(str_schema('text')));
    }

    public function partitionSchema(): Schema
    {
        return $this->fileColumns($this->filesystem, $this->path)->partitions($this->schema ?? new Schema());
    }

    public function source(): Path
    {
        return $this->path;
    }

    public function statistics(): Statistics
    {
        $this->listed ??= ListedFiles::of($this->sourceFiles($this->filesystem, $this->path));

        return new Statistics(size: $this->listed->bytes);
    }

    /**
     * @throws InvalidArgumentException when $schema has no "text" column, the one the lines are read into
     */
    public function withSchema(Schema $schema): static
    {
        if ($schema->findDefinition('text') === null) {
            throw new InvalidArgumentException(sprintf('from_text() reads every line into the "text" column, the schema declares no such column: %s', implode(
                ', ',
                $schema->references()->names(),
            )));
        }

        $this->schema = $schema;

        return $this;
    }
}
