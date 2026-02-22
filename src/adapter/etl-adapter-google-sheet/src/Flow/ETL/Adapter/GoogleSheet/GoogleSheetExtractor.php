<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\{Limitable, LimitableExtractor, Signal};
use Flow\ETL\{Extractor, FlowContext, Schema};
use Google\Service\Sheets;

final class GoogleSheetExtractor implements Extractor, LimitableExtractor
{
    use Limitable;

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

    public function extract(FlowContext $context) : \Generator
    {
        $spreadsheet = $this->service->spreadsheets->get(
            $this->spreadsheetId,
            ['ranges' => [], 'includeGridData' => false]
        );

        $maxRows = 0;

        foreach ($spreadsheet->getSheets() as $sheet) {
            if ($sheet->getProperties()->title === $this->columnRange->sheetName) {
                $maxRows = $sheet->getProperties()->getGridProperties()->getRowCount();

                break;
            }
        }

        $cellsRange = new SheetRange($this->columnRange, 1, $this->rowsPerPage, $maxRows);

        $ranges = [];

        for ($totalRows = 0; $totalRows < $cellsRange->endRow; $totalRows += $this->rowsPerPage) {
            $ranges[] = $cellsRange->toString();

            $cellsRange = $cellsRange->nextRows($this->rowsPerPage);
        }

        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        $headers = [];
        $headersCount = 0;

        $response = $this->service->spreadsheets_values->batchGet($this->spreadsheetId, array_merge($this->options, ['ranges' => $ranges]));

        foreach ($response->getValueRanges() as $valueRange) {
            foreach ($valueRange->getValues() as $rowData) {
                // Skip empty rows
                if ([] === $rowData) {
                    continue;
                }

                $rowDataCount = \count($rowData);

                if ($this->withHeader) {
                    if ([] === $headers) {
                        /** @var array<string> $headers */
                        $headers = $rowData;

                        $headersCount = $rowDataCount;

                        continue;
                    }

                    // Expand columns to the size of the previous row
                    for ($i = $rowDataCount; $i < $headersCount; $i++) {
                        $rowData[$i] = null;
                    }
                }

                if ($rowDataCount > $headersCount) {
                    if (!$this->dropExtraColumns) {
                        throw InvalidArgumentException::because('Row has more columns (%d) than headers (%d)', $rowDataCount, $headersCount);
                    }

                    $rowData = \array_slice($rowData, 0, $headersCount);
                }

                $row = \array_combine($headers, $rowData);

                if ($shouldPutInputIntoRows) {
                    $row['_spread_sheet_id'] = $this->spreadsheetId;
                    $row['_sheet_name'] = $this->columnRange->sheetName;
                }

                $signal = yield array_to_rows($row, $context->entryFactory(), schema: $this->schema);

                $this->incrementReturnedRows();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    return;
                }
            }
        }
    }

    public function withDropExtraColumns(bool $dropExtraColumns) : self
    {
        $this->dropExtraColumns = $dropExtraColumns;

        return $this;
    }

    public function withHeader(bool $withHeader) : self
    {
        $this->withHeader = $withHeader;

        return $this;
    }

    /**
     * @param array{dateTimeRenderOption?: string, majorDimension?: string, valueRenderOption?: string} $options
     */
    public function withOptions(array $options) : self
    {
        $this->options = $options;

        return $this;
    }

    public function withRowsPerPage(int $rowsPerPage) : self
    {
        if ($rowsPerPage < 1) {
            throw new InvalidArgumentException('Rows per page must be greater than 0');
        }

        $this->rowsPerPage = $rowsPerPage;

        return $this;
    }

    public function withSchema(Schema $schema) : self
    {
        $this->schema = $schema;

        return $this;
    }
}
