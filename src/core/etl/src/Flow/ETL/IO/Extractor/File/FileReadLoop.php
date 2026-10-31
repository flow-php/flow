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
        // a format skips by its own counts, which hold only while no conjunct drops a row
        $skips = $batches instanceof OffsetSkippingFileBatches && $conjuncts === [];
        $offset = $skips ? $window->offset : 0;
        $skipping = new ReadWindow($skips ? 0 : $window->offset);
        $skipped = 0;
        $yielded = 0;

        foreach ($sources as $source) {
            $constants = $this->read->fileColumns->forFile($source, $schema);
            // a limit counts the rows the filter keeps, which a format would count before it
            $wanted = $window->limit === null || $conjuncts !== [] ? null : $window->limit - $yielded;
            $file = $batches->batches(
                $this->read,
                $this->criteria,
                $source,
                $body,
                $backend,
                // a format that cannot skip has to produce the offset it leaves to the loop before the rows wanted
                new ReadWindow($offset, $wanted === null ? null : $wanted + $skipping->offset - $skipped),
            );

            foreach ($file as $batch) {
                // a corrupted record is no row: it passes through, outside the filter and the window
                if ($batch instanceof CorruptedRecordException) {
                    $signal = yield $batch;

                    if ($signal === Signal::STOP) {
                        return;
                    }

                    continue;
                }

                $filtered = $filter->filter($constants->fillRows($batch, $schema, $backend), $backend);
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

            $offset -= $file->getReturn();
        }
    }
}
