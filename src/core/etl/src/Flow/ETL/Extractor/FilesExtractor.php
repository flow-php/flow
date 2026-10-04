<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Cardinality;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\File\FileExtractor;
use Flow\ETL\Extractor\File\ListedFiles;
use Flow\ETL\Extractor\File\ListingPartitions;
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows\RowsBuilder;
use Flow\ETL\Schema;
use Flow\Filesystem\FileListing;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Generator;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function sprintf;

final class FilesExtractor implements BatchableExtractor, Extractor, FileExtractor, RewindableExtractor
{
    private ?Schema $schema = null;

    private ?Statistics $statistics = null;

    use Batches;
    use ListingPartitions;

    private readonly Filesystem $filesystem;

    public function __construct(
        private readonly Path $path,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        if (!$filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. files($path, filesystem: aws_s3_filesystem(...)).',
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
     * @return Generator<int, \Flow\ETL\Rows, Signal|null, void>
     */
    public function extract(FlowContext $context, ?int $limit = null, Filter $pathFilter = new OnlyFiles()): Generator
    {
        $batchSize = $this->batchSize();
        $backend = $context->backend();
        $fileColumns = $this->fileColumns($this->filesystem, $this->path);
        $schema = $this->schema();
        $builder = new RowsBuilder($schema, $backend);
        $yielded = 0;

        foreach ((new FileListing($this->filesystem))->list($this->path, $pathFilter) as $fileStatus) {
            $extension = $fileStatus->path->extension();

            $builder->append([
                'path' => $fileStatus->path->path(),
                'protocol' => $fileStatus->path->protocol(),
                'file_name' => $fileStatus->path->filename(),
                'base_name' => $fileStatus->path->basename(),
                'is_file' => $fileStatus->isFile(),
                'is_dir' => $fileStatus->isDirectory(),
                // Path::extension() answers false for an extensionless file; the column is one
                // type, so the absence is spelled null rather than a boolean in a string column.
                'extension' => $extension === false ? null : $extension,
                ...$fileColumns->forFile(new SourceFile($fileStatus->path, $fileStatus->size), $schema)->values(),
            ]);

            if ($builder->count() < $batchSize) {
                continue;
            }

            $rows = $builder->finish();
            $yielded += $rows->count();

            $signal = yield $rows;

            if ($signal === Signal::STOP) {
                return;
            }

            $builder = new RowsBuilder($schema, $backend);

            if ($limit !== null && $yielded >= $limit) {
                return;
            }
        }

        if ($builder->count() > 0) {
            yield $builder->finish();
        }
    }

    public function partitionSchema(): Schema
    {
        return $this->fileColumns($this->filesystem, $this->path)->partitions($this->schema ?? new Schema());
    }

    public function source(): Path
    {
        return $this->path;
    }

    public function schema(): Schema
    {
        $fileColumns = $this->fileColumns($this->filesystem, $this->path);

        if ($this->schema !== null) {
            return $fileColumns->declare($this->schema);
        }

        return $fileColumns->declare(schema(
            str_schema('path'),
            str_schema('protocol'),
            str_schema('file_name'),
            str_schema('base_name'),
            bool_schema('is_file'),
            bool_schema('is_dir'),
            str_schema('extension', nullable: true),
        ));
    }

    public function statistics(): Statistics
    {
        if ($this->statistics === null) {
            $listed = ListedFiles::of((new FileListing($this->filesystem))->list($this->path, new OnlyFiles()));
            $this->statistics = new Statistics(rows: Cardinality::exact($listed->count), size: $listed->bytes);
        }

        return $this->statistics;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
