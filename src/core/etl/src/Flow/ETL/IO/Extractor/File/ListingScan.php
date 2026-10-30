<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\Functions\Evaluation\Filter;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Rows\RowsBuilder;
use Flow\ETL\Values\Schema;
use Generator;

use function array_key_last;

final readonly class ListingScan
{
    public function __construct(
        private SourceFiles $files,
        private FileColumns $fileColumns,
        private ListedFileRow $row,
    ) {}

    /**
     * @param int<1, max> $batchSize
     *
     * @return Generator<int, Rows, null|Signal, void>
     */
    public function scan(Backend $backend, Criteria $criteria, Schema $schema, int $batchSize): Generator
    {
        $filter = Filter::bind($schema, ...$criteria->conjuncts);
        $builder = new RowsBuilder($schema, $backend);
        $skipped = 0;
        $yielded = 0;
        $window = new ReadWindow($criteria->offset, $criteria->limit);
        $listing = $this->files->statuses($criteria);
        $last = array_key_last($listing);

        foreach ($listing as $index => $fileStatus) {
            $builder->appendRows([[
                ...$this->row->row($fileStatus),
                ...$this->fileColumns->forFile(new SourceFile($fileStatus->path, $fileStatus->size), $schema)->values(),
            ]]);

            if ($builder->count() < $batchSize && $index !== $last) {
                continue;
            }

            $filtered = $filter->filter($builder->finish(), $backend);
            $builder = new RowsBuilder($schema, $backend);
            $rows = $window->skip($filtered, $skipped);
            $skipped += $filtered->count() - $rows->count();

            if ($rows->isEmpty()) {
                continue;
            }

            $rows = $window->trim($rows, $yielded);
            $yielded += $rows->count();
            $signal = yield $rows;

            if ($signal === Signal::STOP) {
                return;
            }

            if ($window->reached($yielded)) {
                return;
            }
        }
    }
}
