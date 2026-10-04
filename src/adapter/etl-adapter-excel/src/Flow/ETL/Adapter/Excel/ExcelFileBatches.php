<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

use Flow\ETL\Column\Backend;
use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Extractor\File\FileBatches;
use Flow\ETL\Extractor\File\InferredColumns;
use Flow\ETL\Extractor\File\ReadWindow;
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\Rows;
use Flow\ETL\Rows\RowsBuilder;
use Flow\ETL\Schema;
use Generator;

use function count;

final readonly class ExcelFileBatches implements FileBatches
{
    /**
     * @param null|WorkbookSampler $sample the open sheets of the inference this read follows, read on from where the
     *                                     sample stopped; null to parse every sheet afresh
     * @param null|InferredColumns $inferred the columns every sheet must carry; null when declared or union by name
     */
    public function __construct(
        private WorkbookReader $workbook,
        private ?WorkbookSampler $sample,
        private ?InferredColumns $inferred,
    ) {}

    /**
     * The columns of every sheet must be the ones the inferred schema was derived from - checked before the sheet's
     * first batch is built.
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
        $sheet = $this->sample?->take($source) ?? $this->workbook->sheet($source);

        try {
            $this->inferred?->refuseDivergence($source->uri(), $sheet->columns());

            $batch = [];

            foreach ($sheet->rows() as $rowValues) {
                $batch[] = $rowValues;

                if (count($batch) < $batchSize) {
                    continue;
                }

                yield (new RowsBuilder($body, $backend))
                    ->appendRows($batch)
                    ->finish();

                $batch = [];
            }

            if ($batch !== []) {
                yield (new RowsBuilder($body, $backend))
                    ->appendRows($batch)
                    ->finish();
            }
        } finally {
            $sheet->close();
        }

        return 0;
    }

    /**
     * Closes the sample sheets this read did not take.
     */
    public function close(): void
    {
        $this->sample?->close();
    }
}
