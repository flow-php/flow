<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Functions\Evaluation\Filter;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Generator;

final readonly class FileReadLoop
{
    public function __construct(
        private FileRead $read,
        private Criteria $criteria = new Criteria(),
    ) {}

    /**
     * @param list<SourceFile> $sources
     *
     * @return Generator<int, CorruptedRecordException|Rows, null|Signal, void>
     */
    public function read(array $sources, FileBatches $batches, Backend $backend, ReadWindow $window): Generator
    {
        $schema = $this->read->schema;
        $conjuncts = $this->criteria->conjuncts;
        $body = $this->read->fileColumns->withoutTail($schema);
        $filter = Filter::bind($schema, ...$conjuncts);
        $skipping = new ReadWindow($window->offset);
        $skipped = 0;
        $yielded = 0;

        foreach ($sources as $source) {
            $constants = $this->read->fileColumns->forFile($source, $schema);
            $wanted = $window->limit === null ? null : $window->limit - $yielded;
            $file = $batches->batches(
                $this->read,
                $this->criteria,
                $source,
                $body,
                $backend,
                $batches instanceof OffsetSkippingFileBatches
                    ? new ReadWindow($window->offset - $skipped, $wanted)
                    // a format that cannot skip counts rows before the conjuncts drop them, and the offset with them
                    : new ReadWindow(
                        0,
                        $wanted === null || $conjuncts !== [] ? null : $wanted + $window->offset - $skipped,
                    ),
            );
            $fileFilter = $filter;

            foreach ($file as $batch) {
                if ($batch instanceof SkippedRows) {
                    $skipped += $batch->count;

                    continue;
                }

                if ($batch instanceof Filter) {
                    $fileFilter = $batch;

                    continue;
                }

                // a corrupted record is no row: it passes through, outside the filter and the window
                if ($batch instanceof CorruptedRecordException) {
                    $signal = yield $batch;

                    if ($signal === Signal::STOP) {
                        return;
                    }

                    continue;
                }

                $filtered = $fileFilter->filter($constants->fillRows($batch, $schema, $backend), $backend);
                $rows = $skipping->skip($filtered, $skipped);
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
}
