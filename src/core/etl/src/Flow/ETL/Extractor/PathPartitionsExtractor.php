<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Cardinality;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\File\FileExtractor;
use Flow\ETL\Extractor\File\FileScan;
use Flow\ETL\Extractor\File\ListingPartitions;
use Flow\ETL\Extractor\File\ListingScan;
use Flow\ETL\Extractor\File\PathPartitionsRow;
use Flow\ETL\Extractor\File\Scanning;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\FileListing;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Generator;

use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function iterator_count;
use function sprintf;

final class PathPartitionsExtractor implements BatchableExtractor, Extractor, FileExtractor, RewindableExtractor
{
    private ?Schema $schema = null;

    private ?Statistics $statistics = null;

    use Batches;
    use Scanning;
    use ListingPartitions;

    private readonly Filesystem $filesystem;

    public function __construct(
        private readonly Path $path,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        if (!$filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. from_path_partitions($path, filesystem: aws_s3_filesystem(...)).',
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
    public function scan(FlowContext $context, FileScan $scan): Generator
    {
        return (new ListingScan(
            $this->filesystem,
            $this->path,
            $this->fileColumns($this->filesystem, $this->path),
            new PathPartitionsRow(),
        ))->scan($context, $scan, $this->schema(), $this->batchSize());
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
            map_schema('partitions', type_map(type_string(), type_string())),
        ));
    }

    public function statistics(): Statistics
    {
        return $this->statistics ??=
            new Statistics(rows: Cardinality::exact(iterator_count((new FileListing($this->filesystem))->list(
                $this->path,
                new OnlyFiles(),
            ))));
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
