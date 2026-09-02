<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Extractor\MetadataColumns;
use Flow\ETL\Extractor\MetadataColumnsExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;
use Google\Service\Sheets;

use function count;
use function Flow\ETL\DSL\str_schema;

final class GoogleSheetExtractor implements Extractor, LimitableExtractor, MetadataColumnsExtractor
{
    use Limitable;
    use MetadataColumns;

    private bool $dropExtraColumns = true;

    /**
     * @var array{dateTimeRenderOption?: string, majorDimension?: string, valueRenderOption?: string}
     */
    private array $options = [];

    private int $rowsPerPage = 1000;

    private ?Schema $schema = null;

    private bool $withHeader = true;

    public function __construct(
        private readonly Sheets $service,
        private readonly string $spreadsheetId,
        private readonly Columns $columnRange,
    ) {
        $this->resetLimit();
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        /** @var Sheets\Resource\Spreadsheets $spreadsheetsResource */
        $spreadsheetsResource = $this->service->spreadsheets;

        $spreadsheet = $spreadsheetsResource->get($this->spreadsheetId, [
            'ranges' => [],
            'includeGridData' => false,
        ]);

        $maxRows = 0;

        foreach ($spreadsheet->getSheets() as $sheet) {
            $properties = $sheet->getProperties();

            if ($properties->title === $this->columnRange->sheetName) {
                $maxRows = $properties->getGridProperties()->getRowCount();

                break;
            }
        }

        $cellsRange = new SheetRange($this->columnRange, 1, $this->rowsPerPage, $maxRows);

        $ranges = [];

        for ($totalRows = 0; $totalRows < $cellsRange->endRow; $totalRows += $this->rowsPerPage) {
            $ranges[] = $cellsRange->toString();

            $cellsRange = $cellsRange->nextRows($this->rowsPerPage);
        }
        $hydrator = $context->hydrator();
        $batchSize = $context->config->extractorBatchSize();

        $schema = $this->schema === null ? null : $this->schema();

        /** @var Sheets\Resource\SpreadsheetsValues $valuesResource */
        $valuesResource = $this->service->spreadsheets_values;

        $response = $valuesResource->batchGet($this->spreadsheetId, array_merge($this->options, [
            'ranges' => $ranges,
        ]));

        $encoder = new GoogleSheetEncoder(withHeader: $this->withHeader, dropExtraColumns: $this->dropExtraColumns);
        $rawRows = [];

        foreach ($response->getValueRanges() as $valueRange) {
            // @mago-ignore analysis:redundant-null-coalesce
            foreach ($valueRange->getValues() ?? [] as $rowData) {
                $rawRows[] = $rowData;

                if (count($rawRows) >= $batchSize) {
                    $batch = [];

                    foreach ($encoder->decode($rawRows) as $rowValues) {
                        $row = $rowValues->values;

                        if ($this->addMetadataColumns) {
                            $row['_spread_sheet_id'] = $this->spreadsheetId;
                            $row['_sheet_name'] = $this->columnRange->sheetName;
                        }

                        $batch[] = new RawRowValues($row);
                    }

                    $rawRows = [];

                    $hydrated = $hydrator->cast($batch, $schema);

                    foreach ($hydrated as $hydratedRow) {
                        $signal = yield Rows::trusted($hydrated->schema(), [$hydratedRow]);

                        $this->incrementReturnedRows();

                        if ($signal === Signal::STOP || $this->reachedLimit()) {
                            return;
                        }
                    }
                }
            }
        }

        $batch = [];

        foreach ($encoder->decode($rawRows) as $rowValues) {
            $row = $rowValues->values;

            if ($this->addMetadataColumns) {
                $row['_spread_sheet_id'] = $this->spreadsheetId;
                $row['_sheet_name'] = $this->columnRange->sheetName;
            }

            $batch[] = new RawRowValues($row);
        }

        $hydrated = $hydrator->cast($batch, $schema);

        foreach ($hydrated as $hydratedRow) {
            $signal = yield Rows::trusted($hydrated->schema(), [$hydratedRow]);

            $this->incrementReturnedRows();

            if ($signal === Signal::STOP || $this->reachedLimit()) {
                return;
            }
        }
    }

    public function schema(): Schema
    {
        if ($this->schema === null) {
            throw SchemaNotDerivableException::extractor(self::class);
        }

        if ($this->addMetadataColumns) {
            return $this->schema->add(str_schema('_spread_sheet_id'), str_schema('_sheet_name'));
        }

        return $this->schema;
    }

    public function withDropExtraColumns(bool $dropExtraColumns): self
    {
        $this->dropExtraColumns = $dropExtraColumns;

        return $this;
    }

    public function withHeader(bool $withHeader): self
    {
        $this->withHeader = $withHeader;

        return $this;
    }

    /**
     * @param array{dateTimeRenderOption?: string, majorDimension?: string, valueRenderOption?: string} $options
     */
    public function withOptions(array $options): self
    {
        $this->options = $options;

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
