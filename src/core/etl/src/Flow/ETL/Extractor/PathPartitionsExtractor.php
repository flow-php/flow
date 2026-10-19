<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Cardinality;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\File\FileExtractor;
use Flow\ETL\Extractor\File\FileScan;
use Flow\ETL\Extractor\File\ListingPartitions;
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Conjuncts;
use Flow\ETL\Rows;
use Flow\ETL\Rows\RowsBuilder;
use Flow\ETL\Schema;
use Flow\ETL\Transformer\ScalarFunctionFilterTransformer;
use Flow\Filesystem\FileListing;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Generator;

use function array_map;
use function array_merge;
use function array_values;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function iterator_count;
use function sprintf;

final class PathPartitionsExtractor implements BatchableExtractor, Extractor, FileExtractor, RewindableExtractor
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
    public function extract(FlowContext $context, ?int $limit = null): Generator
    {
        return $this->scan($context, new FileScan(limit: $limit));
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function scan(FlowContext $context, FileScan $scan): Generator
    {
        $batchSize = $this->batchSize();
        $backend = $context->backend();
        $fileColumns = $this->fileColumns($this->filesystem, $this->path);
        $schema = $this->schema();
        $builder = new RowsBuilder($schema, $backend);
        $filters = [];

        foreach ($scan->filter === null ? [] : (new Conjuncts())->of($scan->filter) as $conjunct) {
            $filters[] = (new ScalarFunctionFilterTransformer($conjunct))->bound($schema);
        }

        $yielded = 0;
        $listing = (new FileListing($this->filesystem))->list($this->path, $scan->pathFilter);

        while ($listing->valid()) {
            $fileStatus = type_instance_of(FileStatus::class)->assert($listing->current());
            $listing->next();

            $builder->append([
                'path' => $fileStatus->path->uri(),
                'partitions' => array_merge(...array_values(array_map(static fn(Partition $p) => [
                    $p->name => $p->value,
                ], $fileStatus->path->partitions()->toArray()))),
                ...$fileColumns->forFile(new SourceFile($fileStatus->path, $fileStatus->size), $schema)->values(),
            ]);

            if ($builder->count() < $batchSize && $listing->valid()) {
                continue;
            }

            $rows = $builder->finish();
            $builder = new RowsBuilder($schema, $backend);

            foreach ($filters as $filter) {
                $rows = $filter->transform($rows, $context);
            }

            if ($rows->isEmpty()) {
                continue;
            }

            $yielded += $rows->count();

            $signal = yield $rows;

            if ($signal === Signal::STOP) {
                return;
            }

            if ($scan->limit !== null && $yielded >= $scan->limit) {
                return;
            }
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
