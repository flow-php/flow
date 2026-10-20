<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\File;

use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\Filter;
use Flow\ETL\Rows;
use Flow\ETL\Rows\RowsBuilder;
use Flow\ETL\Schema;
use Flow\Filesystem\FileListing;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Generator;

use function Flow\Types\DSL\type_instance_of;

final readonly class ListingScan
{
    public function __construct(
        private Filesystem $filesystem,
        private Path $path,
        private FileColumns $fileColumns,
        private ListedFileRow $row,
    ) {}

    /**
     * @param int<1, max> $batchSize
     *
     * @return Generator<int, Rows, null|Signal, void>
     */
    public function scan(FlowContext $context, FileScan $scan, Schema $schema, int $batchSize): Generator
    {
        $backend = $context->backend();
        $filter = Filter::bind($schema, ...$scan->conjuncts);
        $builder = new RowsBuilder($schema, $backend);
        $yielded = 0;
        $listing = (new FileListing($this->filesystem))->list($this->path, $scan->pathFilter);

        while ($listing->valid()) {
            $fileStatus = type_instance_of(FileStatus::class)->assert($listing->current());
            $listing->next();

            $builder->append([
                ...$this->row->row($fileStatus),
                ...$this->fileColumns->forFile(new SourceFile($fileStatus->path, $fileStatus->size), $schema)->values(),
            ]);

            if ($builder->count() < $batchSize && $listing->valid()) {
                continue;
            }

            $rows = $filter->filter($builder->finish(), $context);
            $builder = new RowsBuilder($schema, $backend);

            if ($rows->isEmpty()) {
                continue;
            }

            if ($scan->limit !== null && $rows->count() > ($scan->limit - $yielded)) {
                $rows = $rows->slice(0, $scan->limit - $yielded);
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
}
