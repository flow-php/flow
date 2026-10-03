<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Column\Backend;
use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Extractor\File\FileBatches;
use Flow\ETL\Extractor\File\InferredColumns;
use Flow\ETL\Extractor\File\ReadWindow;
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

final readonly class CSVFileBatches implements FileBatches
{
    /**
     * @param null|InferredColumns $inferred the columns every file must carry; null when declared or union by name
     */
    public function __construct(
        private CSVFileReader $reader,
        private ?InferredColumns $inferred,
    ) {}

    /**
     * The columns of every file must be the ones the inferred schema was derived from - checked before the file's
     * first batch is built, so a cell the inferred types refuse cannot hide a file whose columns diverge.
     *
     * @param int<1, max> $batchSize
     *
     * @throws InferredSchemaException
     *
     * @return Generator<int, Rows, mixed, int>
     */
    public function batches(
        SourceFile $source,
        Schema $body,
        int $batchSize,
        Backend $backend,
        ReadWindow $window,
    ): Generator {
        $this->inferred?->refuseDivergence($source->uri(), $this->reader->columns($source));

        yield from $this->reader->batches($source, $body, $batchSize, $backend);

        return 0;
    }
}
