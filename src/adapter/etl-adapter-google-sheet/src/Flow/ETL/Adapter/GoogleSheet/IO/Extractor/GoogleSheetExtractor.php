<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\IO\Extractor;

use Flow\ETL\Adapter\GoogleSheet\IO\Inference\GoogleSheetSniffer;
use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaValidationException;
use Flow\ETL\IO\CorruptedRecordsRouter;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\BatchableExtractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\MetadataColumnsExtractor;
use Flow\ETL\IO\Extractor\InfersSchema;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\IO\Inference\SchemaInference;
use Flow\ETL\IO\Inference\SchemaInferenceBuilder;
use Flow\ETL\IO\Inference\SchemaInferrer;
use Flow\ETL\IO\Loader;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Rows\RecordBatch;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition\StringDefinition;
use Generator;
use Google\Service\Sheets;

use function array_diff;
use function is_string;
use function sprintf;

/**
 * @import-type GoogleSheetOptions from GoogleSheetReadOptions
 */
final class GoogleSheetExtractor implements
    BatchableExtractor,
    Extractor,
    InfersSchema,
    MetadataColumnsExtractor,
    CorruptedRecordsRouter
{
    private ?Loader $corruptedRecordsLoader = null;

    private BatchSize $batchSize;

    /**
     * A sheet range is an HTTP request clamped to the grid, so the core default would sample the whole sheet.
     */
    private const int SAMPLE_ROWS = 100;

    private bool $addMetadataColumns = false;

    private ?Schema $derivedSchema = null;

    private SchemaInference $inference;

    private GoogleSheetReadOptions $readOptions;

    private int $rowsPerPage = 1000;

    /**
     * The grid row count the last schema inference fetched; null until one ran.
     */
    private ?int $gridRowCount = null;

    private ?Schema $schema = null;

    private ?Statistics $statistics = null;

    public function __construct(
        private readonly Sheets $service,
        private readonly string $spreadsheetId,
        private readonly Columns $columnRange,
    ) {
        $this->inference = new SchemaInference(sampleSize: self::SAMPLE_ROWS);
        $this->readOptions = new GoogleSheetReadOptions();
        $this->batchSize = new BatchSize();
    }

    /**
     * @return int<1, max>
     */
    public function batchSize(): int
    {
        return $this->batchSize->size;
    }

    public function isRepeatable(): bool
    {
        return true;
    }

    public function corruptedRecordsLoader(): ?Loader
    {
        return $this->corruptedRecordsLoader;
    }

    /**
     * @return Generator<int, CorruptedRecordException|Rows, Signal|null, void>
     */
    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        $reader = new GoogleSheetReader($this->service, $this->spreadsheetId, $this->columnRange, $this->readOptions);
        $sniffer = new GoogleSheetSniffer(
            $reader,
            $this->inference->sampleSize,
            $this->readOptions->typer($this->inference->candidates()),
        );
        // only THIS call's sample can serve the read; a memo hit means no rows were fetched here
        $sampledNow = $this->schema === null && $this->derivedSchema === null;
        $base = $this->schema ?? ($this->derivedSchema ??= (new SchemaInferrer($this->inference))->infer(
            $sniffer,
        )->schema);

        if ($sampledNow) {
            $this->gridRowCount = $sniffer->gridRowCount();
        }

        $schema = $this->addMetadataColumns
            ? $base->add(new StringDefinition('_spread_sheet_id'), new StringDefinition('_sheet_name'))
            : $base;
        $checked = $this->schema === null && $this->inference->unionByName;

        $yielded = 0;
        // the sample already holds every row when its range covered the grid, so the read is served from it rather
        // than fetching the same rows again; nothing can have diverged from a schema read off these very rows
        $sheetRows = ($sampledNow ? $sniffer->rows() : null) ?? $reader->rows($this->rowsPerPage);
        $uri = sprintf('google-sheet://%s/%s', $this->spreadsheetId, $this->columnRange->sheetName);
        $batch = $this->schema === null
            ? RecordBatch::inferred($schema, $backend, $this->batchSize(), $uri, $this->inference->sampleSize)
            : new RecordBatch($schema, $backend, $this->batchSize(), $uri);

        foreach ($this->recordBatches($batch, $uri, $sheetRows, $base, $checked) as $rows) {
            $yielded += $rows instanceof Rows ? $rows->count() : 0;

            $signal = yield $rows;

            if ($signal === Signal::STOP) {
                return;
            }

            if ($criteria->limit !== null && $yielded >= $criteria->limit) {
                return;
            }
        }
    }

    public function inferSchema(SchemaInferenceBuilder $builder): static
    {
        $this->inference = $builder->build();
        $this->derivedSchema = null;
        $this->gridRowCount = null;
        $this->statistics = null;

        return $this;
    }

    public function schema(): Schema
    {
        $sniffer = new GoogleSheetSniffer(
            new GoogleSheetReader($this->service, $this->spreadsheetId, $this->columnRange, $this->readOptions),
            $this->inference->sampleSize,
            $this->readOptions->typer($this->inference->candidates()),
        );
        $sampledNow = $this->schema === null && $this->derivedSchema === null;
        $base = $this->schema ?? ($this->derivedSchema ??= (new SchemaInferrer($this->inference))->infer(
            $sniffer,
        )->schema);

        if ($sampledNow) {
            $this->gridRowCount = $sniffer->gridRowCount();
        }

        if ($this->schema === null && $base->count() === 0) {
            throw InferredSchemaException::nothingToInferFrom('from_google_sheet()', sprintf(
                'spreadsheet "%s" sheet "%s" carries no column',
                $this->spreadsheetId,
                $this->columnRange->sheetName,
            ));
        }

        return $this->addMetadataColumns
            ? $base->add(new StringDefinition('_spread_sheet_id'), new StringDefinition('_sheet_name'))
            : $base;
    }

    /**
     * The grid's allocated rows bound the populated ones; they come from the sample schema inference already
     * fetched, and without one nothing is declared - no request is made just for them. The Sheets API reports no
     * byte size.
     */
    public function statistics(): Statistics
    {
        if ($this->gridRowCount === null) {
            return new Statistics();
        }

        return $this->statistics ??= new Statistics(rows: Cardinality::atMost($this->gridRowCount));
    }

    public function withBatchSize(int $batchSize): static
    {
        $this->batchSize = new BatchSize($batchSize);

        return $this;
    }

    public function withCorruptedRecords(?Loader $loader): static
    {
        $this->corruptedRecordsLoader = $loader;

        return $this;
    }

    public function withMissingColumnsAsNull(bool $missingColumnsAsNull): self
    {
        $this->readOptions = $this->readOptions->withMissingColumnsAsNull($missingColumnsAsNull);
        $this->derivedSchema = null;
        $this->gridRowCount = null;
        $this->statistics = null;

        return $this;
    }

    public function withEmptyToNull(bool $emptyToNull): self
    {
        $this->readOptions = $this->readOptions->withEmptyToNull($emptyToNull);
        $this->derivedSchema = null;
        $this->gridRowCount = null;
        $this->statistics = null;

        return $this;
    }

    public function withHeader(bool $withHeader): self
    {
        $this->readOptions = $this->readOptions->withHeader($withHeader);
        $this->derivedSchema = null;
        $this->gridRowCount = null;
        $this->statistics = null;

        return $this;
    }

    public function withMetadataColumns(bool $addMetadataColumns): static
    {
        $this->addMetadataColumns = $addMetadataColumns;

        return $this;
    }

    /**
     * @param GoogleSheetOptions $options
     */
    public function withOptions(array $options): self
    {
        $this->readOptions = $this->readOptions->withOptions($options);
        $this->derivedSchema = null;
        $this->gridRowCount = null;
        $this->statistics = null;

        return $this;
    }

    public function withRowsPerPage(int $rowsPerPage): self
    {
        if ($rowsPerPage < 1) {
            throw new InvalidArgumentException('Rows per page must be greater than 0');
        }

        $this->rowsPerPage = $rowsPerPage;

        return $this;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }

    /**
     * @param Generator<int, list<mixed>> $sheetRows the raw cells of every row, keyed by its number in the sheet
     *
     * @throws InferredSchemaException when the sheet's header diverges from the inferred schema
     *
     * @return Generator<int, CorruptedRecordException|Rows>
     */
    private function recordBatches(
        RecordBatch $batch,
        string $uri,
        Generator $sheetRows,
        Schema $base,
        bool $checked,
    ): Generator {
        $decoder = $this->readOptions->decoder();
        $sheet = sprintf('spreadsheet "%s" sheet "%s"', $this->spreadsheetId, $this->columnRange->sheetName);

        foreach ($sheetRows as $number => $cells) {
            $values = $decoder->decode($cells);
            $columns = $decoder->headers();

            if (!$checked && $columns !== null) {
                $checked = true;
                $expected = $base->names();
                $missing = $base->missingFrom($columns);

                if ($this->schema !== null && $missing !== []) {
                    throw SchemaValidationException::declaredColumnsMissing($uri, $missing, $columns);
                }

                if ($this->schema === null && (array_diff($columns, $expected) !== [] || $missing !== [])) {
                    // a sheet is one source, so it is named as both the read and the inferred-from side
                    throw InferredSchemaException::columnsDiverge(
                        $sheet,
                        $sheet,
                        $expected,
                        $columns,
                        $this->inference->sampleSize,
                        $this->inference->filesToSniff,
                    );
                }
            }

            if ($values === null) {
                continue;
            }

            if (is_string($values)) {
                yield from $batch->refuse(CorruptedRecordException::fromCells($uri, $number, null, $values, $cells));

                continue;
            }

            if ($this->addMetadataColumns) {
                // assigned, never unpacked: a numeric column name is an int array key and ... renumbers it
                $values['_spread_sheet_id'] = $this->spreadsheetId;
                $values['_sheet_name'] = $this->columnRange->sheetName;
            }

            yield from $batch->add($values, $number, fields: $cells);
        }

        yield from $batch->flush();
    }
}
