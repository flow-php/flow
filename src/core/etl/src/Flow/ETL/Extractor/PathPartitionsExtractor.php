<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Schema;
use Flow\Filesystem\FileListing;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Path;
use Generator;

use function array_map;
use function array_merge;
use function array_values;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_entry;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function sprintf;

final class PathPartitionsExtractor implements Extractor, FileExtractor, LimitableExtractor
{
    private ?Schema $schema = null;

    use Limitable;
    use PathFiltering;

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

    /**
     * @return Generator<int, \Flow\ETL\Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        foreach ((new FileListing($this->filesystem))->list($this->path, $this->filter()) as $fileStatus) {
            $partitions = $fileStatus->path->partitions();

            $row = row(
                string_entry('path', $fileStatus->path->uri()),
                map_entry(
                    'partitions',
                    array_merge(...array_values(array_map(static fn(Partition $p) => [
                        $p->name => $p->value,
                    ], $partitions->toArray()))),
                    type_map(type_string(), type_string()),
                ),
            );

            $batch = rows($row);

            if ($this->schema !== null) {
                $batch = array_to_rows($batch->toArray(), $context->hydrator(), $batch->partitions(), $this->schema);
            }

            $signal = yield $batch;

            $this->incrementReturnedRows();

            if ($signal === Signal::STOP || $this->reachedLimit()) {
                return;
            }
        }
    }

    public function source(): Path
    {
        return $this->path;
    }

    public function schema(): Schema
    {
        if ($this->schema !== null) {
            return $this->schema;
        }

        return schema(str_schema('path'), map_schema('partitions', type_map(type_string(), type_string())));
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
