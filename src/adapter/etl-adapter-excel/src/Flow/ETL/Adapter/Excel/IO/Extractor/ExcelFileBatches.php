<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\IO\Extractor;

use Flow\ETL\Adapter\Excel\IO\Inference\ExcelSniffer;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileBatches;
use Flow\ETL\IO\Extractor\File\FileRead;
use Flow\ETL\IO\Extractor\File\InferredColumns;
use Flow\ETL\IO\Extractor\File\ReadWindow;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Rows\RecordBatch;
use Flow\ETL\Values\Schema;
use Generator;
use SensitiveParameter;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_optional;

final readonly class ExcelFileBatches implements FileBatches
{
    /**
     * @param null|ExcelSniffer $sample the open sheets of the inference this read follows, read on from where the
     *                                     sample stopped; null to parse every sheet afresh; never described
     * @param null|InferredColumns $inferred the columns every sheet must carry; null when declared or union by name
     */
    public function __construct(
        private WorkbookReader $workbook,
        private ?ExcelSniffer $sample,
        private ?InferredColumns $inferred,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        $inferred = type_optional(type_array())->assert($data['inferred'] ?? null);

        return new self(
            $rebuild->of($data['workbook'] ?? null, WorkbookReader::class),
            null,
            $inferred === null ? null : InferredColumns::fromArray($inferred),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'workbook' => $this->workbook,
            'inferred' => $this->inferred?->normalize(),
        ]);
    }

    /**
     * The columns of every sheet must be the ones the inferred schema was derived from - checked before the sheet's
     * first batch is built.
     *
     * @throws InferredSchemaException
     *
     * @return Generator<int, Rows, mixed, int>
     */
    public function batches(
        FileRead $read,
        Criteria $criteria,
        SourceFile $source,
        Schema $body,
        Backend $backend,
        ReadWindow $window,
    ): Generator {
        $sheet = $this->sample?->take($source) ?? $this->workbook->sheet($source);

        try {
            $this->inferred?->refuseDivergence($source->uri(), $sheet->columns());

            $batch = new RecordBatch($body, $backend, $read->batchSize);

            foreach ($sheet->rows() as $rowValues) {
                $rows = $batch->add($rowValues);

                if ($rows !== null) {
                    yield $rows;
                }
            }

            $rest = $batch->flush();

            if ($rest !== null) {
                yield $rest;
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
