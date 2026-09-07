<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\InfersSchema;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Extractor\MetadataColumns;
use Flow\ETL\Extractor\MetadataColumnsExtractor;
use Flow\ETL\Extractor\RewindableExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Schema\Inference\SchemaInferenceBuilder;
use Flow\ETL\Schema\Inference\SchemaInferrer;
use Generator;
use Google\Service\Sheets;

use function array_diff;
use function array_keys;
use function array_map;
use function Flow\ETL\DSL\str_schema;
use function sprintf;

/**
 * @import-type GoogleSheetOptions from GoogleSheetReadOptions
 */
final class GoogleSheetExtractor implements
    Extractor,
    InfersSchema,
    LimitableExtractor,
    MetadataColumnsExtractor,
    RewindableExtractor
{
    use Limitable;
    use MetadataColumns;

    /**
     * Core defaults to 20 480. A sheet range is an HTTP
     * request and the range clamps to the grid, so that default samples the WHOLE sheet for anything under
     * ~20 000 rows - and the read then fetches it again. Polars' 100 keeps the sample proportionally small.
     * Widen it per read with ->inferSchema(infer_schema()->sampleSize(...)), or -1 for the whole sheet.
     */
    private const int SAMPLE_ROWS = 100;

    private ?Schema $derivedSchema = null;

    private SchemaInference $inference;

    private GoogleSheetReadOptions $readOptions;

    private int $rowsPerPage = 1000;

    private ?Schema $schema = null;

    public function __construct(
        private readonly Sheets $service,
        private readonly string $spreadsheetId,
        private readonly Columns $columnRange,
    ) {
        $this->inference = new SchemaInference(sampleSize: self::SAMPLE_ROWS);
        $this->readOptions = new GoogleSheetReadOptions();
        $this->resetLimit();
    }

    public function isRepeatable(): bool
    {
        return true;
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $reader = new GoogleSheetReader($this->service, $this->spreadsheetId, $this->columnRange, $this->readOptions);
        $sampler = new GoogleSheetSampler($reader, $this->inference->sampleSize);
        // only THIS call's sample can serve the read; a memo hit means no rows were fetched here
        $sampledNow = $this->schema === null && $this->derivedSchema === null;
        $base = $this->schema ?? ($this->derivedSchema ??= (new SchemaInferrer(
            $this->inference,
            $this->readOptions->typer($this->inference->candidates()),
        ))->infer($sampler->header(), $sampler->samples($this->inference->sampleSize)));
        $schema = $this->addMetadataColumns
            ? $base->add(str_schema('_spread_sheet_id'), str_schema('_sheet_name'))
            : $base;
        $hydrator = $context->hydrator();
        $checked = $this->schema !== null || $this->inference->unionByName;

        $batchSize = $context->config->extractorBatchSize();
        // the sample already holds every row when its range covered the grid, so the read is served from it rather
        // than fetching the same rows again; nothing can have diverged from a schema read off these very rows
        $batches = ($sampledNow ? $sampler->batches($batchSize) : null) ?? $reader->batches(
            $this->rowsPerPage,
            $batchSize,
        );

        foreach ($batches as $batch) {
            if (!$checked) {
                $columns = array_map(
                    static fn(int|string $name): string => (string) $name,
                    array_keys($batch[0]->values),
                );

                // a row that carries no columns is not a divergence: withHeader(false) decodes a leading blank
                // row to one, and the names come from the row after it
                if ($columns !== []) {
                    $expected = $base->references()->names();

                    if (array_diff($columns, $expected) !== [] || array_diff($expected, $columns) !== []) {
                        // a sheet is one source, so it is named as both the read and the inferred-from side
                        throw InferredSchemaException::columnsDiverge(
                            sprintf('spreadsheet "%s" sheet "%s"', $this->spreadsheetId, $this->columnRange->sheetName),
                            sprintf('spreadsheet "%s" sheet "%s"', $this->spreadsheetId, $this->columnRange->sheetName),
                            $base,
                            $columns,
                            $this->inference,
                        );
                    }

                    $checked = true;
                }
            }

            if ($this->addMetadataColumns) {
                $batch = array_map(
                    function (RawRowValues $rowValues): RawRowValues {
                        // assigned, never unpacked: a numeric column name is an int array key and ... renumbers it
                        $values = $rowValues->values;
                        $values['_spread_sheet_id'] = $this->spreadsheetId;
                        $values['_sheet_name'] = $this->columnRange->sheetName;

                        return new RawRowValues($values);
                    },
                    $batch,
                );
            }

            $hydrated = $hydrator->hydrate($batch, $schema);

            foreach ($hydrated as $hydratedRow) {
                $signal = yield Rows::trusted($hydrated->schema(), [$hydratedRow]);

                $this->incrementReturnedRows();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    return;
                }
            }
        }
    }

    public function inferSchema(SchemaInferenceBuilder $builder): static
    {
        $this->inference = $builder->build();
        $this->derivedSchema = null;

        return $this;
    }

    public function schema(): Schema
    {
        $sampler = new GoogleSheetSampler(
            new GoogleSheetReader($this->service, $this->spreadsheetId, $this->columnRange, $this->readOptions),
            $this->inference->sampleSize,
        );
        $base = $this->schema ?? ($this->derivedSchema ??= (new SchemaInferrer(
            $this->inference,
            $this->readOptions->typer($this->inference->candidates()),
        ))->infer($sampler->header(), $sampler->samples($this->inference->sampleSize)));

        return $this->addMetadataColumns
            ? $base->add(str_schema('_spread_sheet_id'), str_schema('_sheet_name'))
            : $base;
    }

    public function withDropExtraColumns(bool $dropExtraColumns): self
    {
        $this->readOptions = $this->readOptions->withDropExtraColumns($dropExtraColumns);
        $this->derivedSchema = null;

        return $this;
    }

    public function withEmptyToNull(bool $emptyToNull): self
    {
        $this->readOptions = $this->readOptions->withEmptyToNull($emptyToNull);
        $this->derivedSchema = null;

        return $this;
    }

    public function withHeader(bool $withHeader): self
    {
        $this->readOptions = $this->readOptions->withHeader($withHeader);
        $this->derivedSchema = null;

        return $this;
    }

    /**
     * @param GoogleSheetOptions $options
     */
    public function withOptions(array $options): self
    {
        $this->readOptions = $this->readOptions->withOptions($options);
        $this->derivedSchema = null;

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
}
