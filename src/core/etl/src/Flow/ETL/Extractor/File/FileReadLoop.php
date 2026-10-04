<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\File;

use Flow\ETL\Column\Backend;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function min;

final readonly class FileReadLoop
{
    public function __construct(
        private FileColumns $fileColumns,
        private Schema $schema,
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
        Backend $backend,
        ReadWindow $window,
    ): Generator {
        $body = $this->fileColumns->withoutTail($this->schema);
        $offset = $window->offset;
        $skips = $batches instanceof OffsetSkippingFileBatches;
        $yielded = 0;

        foreach ($sources as $source) {
            $constants = $this->fileColumns->forFile($source, $this->schema);
            $wanted = $window->limit === null ? null : $window->limit - $yielded;
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

                if ($window->limit !== null && $batch->count() > ($window->limit - $yielded)) {
                    $batch = $batch->slice(0, $window->limit - $yielded);
                }

                $rows = $constants->fillRows($batch, $this->schema, $backend);
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
