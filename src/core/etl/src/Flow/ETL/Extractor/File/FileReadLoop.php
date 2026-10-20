<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\File;

use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\Filter;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function min;

final readonly class FileReadLoop
{
    /**
     * @param list<ScalarFunction> $conjuncts FileScan::$conjuncts
     */
    public function __construct(
        private FileColumns $fileColumns,
        private Schema $schema,
        private array $conjuncts = [],
    ) {}

    /**
     * @param list<SourceFile> $sources
     * @param int<1, max> $batchSize
     *
     * @return Generator<int, Rows, null|Signal, void>
     */
    public function read(
        array $sources,
        FileBatches $batches,
        int $batchSize,
        FlowContext $context,
        ReadWindow $window,
    ): Generator {
        $body = $this->fileColumns->withoutTail($this->schema);
        $backend = $context->backend();
        $filter = Filter::bind($this->schema, ...$this->conjuncts);
        $offset = $window->offset;
        $skips = $batches instanceof OffsetSkippingFileBatches;
        $yielded = 0;

        foreach ($sources as $source) {
            $constants = $this->fileColumns->forFile($source, $this->schema);
            // a limit counts the rows the filter keeps, which a format would count before it
            $wanted = $window->limit === null || $this->conjuncts !== [] ? null : $window->limit - $yielded;
            $file = $batches->batches(
                $source,
                $body,
                $batchSize,
                $backend,
                // a format that cannot skip has to produce the offset it leaves to the loop before the rows wanted
                $skips
                    ? new ReadWindow($offset, $wanted)
                    : new ReadWindow(0, $wanted === null ? null : $wanted + $offset),
            );

            foreach ($file as $batch) {
                if (!$skips && $offset > 0) {
                    $skipped = min($offset, $batch->count());
                    $offset -= $skipped;

                    if ($skipped === $batch->count()) {
                        continue;
                    }

                    $batch = $batch->slice($skipped, $batch->count() - $skipped);
                }

                $rows = $filter->filter($constants->fillRows($batch, $this->schema, $backend), $context);

                if ($rows->isEmpty()) {
                    continue;
                }

                if ($window->limit !== null && $rows->count() > ($window->limit - $yielded)) {
                    $rows = $rows->slice(0, $window->limit - $yielded);
                }

                $yielded += $rows->count();
                $signal = yield $rows;

                if ($signal === Signal::STOP) {
                    return;
                }

                if ($window->limit !== null && $yielded >= $window->limit) {
                    return;
                }
            }

            $offset -= $file->getReturn();
        }
    }
}
