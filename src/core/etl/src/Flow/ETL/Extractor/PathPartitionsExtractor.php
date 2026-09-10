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
use function count;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function sprintf;

final class PathPartitionsExtractor implements
    BatchableExtractor,
    Extractor,
    FileExtractor,
    LimitPushDown,
    RewindableExtractor
{
    private ?Schema $schema = null;

    use Batches;
    use PathFiltering;
    use PushesLimit;

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
     * @return Generator<int, \Flow\ETL\Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $batchSize = $this->batchSize();
        $schema = $this->schema();
        $buffer = [];
        $yielded = 0;

        foreach ((new FileListing($this->filesystem))->list($this->path, $this->filter()) as $fileStatus) {
            $buffer[] = [
                'path' => $fileStatus->path->uri(),
                'partitions' => array_merge(...array_values(array_map(static fn(Partition $p) => [
                    $p->name => $p->value,
                ], $fileStatus->path->partitions()->toArray()))),
            ];

            if (count($buffer) < $batchSize) {
                continue;
            }

            $yielded += count($buffer);

            $signal = yield array_to_rows($buffer, $schema, $context->hydrator());

            if ($signal === Signal::STOP) {
                return;
            }

            $buffer = [];

            $limit = $this->pushedLimit();

            if ($limit !== null && $yielded >= $limit) {
                return;
            }
        }

        if ($buffer !== []) {
            yield array_to_rows($buffer, $schema, $context->hydrator());
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
