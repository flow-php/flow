<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\{Limitable, LimitableExtractor, Signal};
use Flow\ETL\{Extractor, FlowContext};
use Google\Service\Sheets;

final class GoogleSheetExtractor implements Extractor, LimitableExtractor
{
    use Limitable;

    private bool $dropExtraColumns = true;

    /**
     * @param array{dateTimeRenderOption?: string, majorDimension?: string, valueRenderOption?: string} $options
     */
    private array $options = [];

    private int $rowsPerPage = 1000;

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
        $cellsRange = new SheetRange($this->columnRange, 1, $this->rowsPerPage);
        $headers = [];

        /** @var Sheets\ValueRange $response */
        $response = $this->service->spreadsheets_values->get(
            $this->spreadsheetId,
            $cellsRange->toString(),
            $this->options
        );

        /**
         * @var array<array> $values
         */
        $values = $response->getValues() ?? [];

        if ($this->withHeader && [] !== $values) {
            /** @var array<string> $headers */
            $headers = $values[0];
            unset($values[0]);
            $totalRows = 1;
        } else {
            $totalRows = 0;
        }

        $headersCount = \count($headers);

        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        while (\count($values)) {
            $rows = \array_map(
                function (array $rowData) use ($headers, $headersCount, $shouldPutInputIntoRows) {
                    $rowDataCount = \count($rowData);

                    if ($headersCount > $rowDataCount) {
                        \array_push(
                            $rowData,
                            ...\array_map(
                                static fn (int $i) => null,
                                \range(1, $headersCount - $rowDataCount)
                            )
                        );
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

                    return $row;
                },
                $values
            );

            $totalRows += \count($rows);

            foreach ($rows as $row) {
                $signal = yield array_to_rows($row, $context->entryFactory());
                $this->incrementReturnedRows();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    return;
                }
            }

            if ($totalRows < $cellsRange->endRow) {
                return;
            }

            $cellsRange = $cellsRange->nextRows($this->rowsPerPage);

            $response = $this->service->spreadsheets_values->get($this->spreadsheetId, $cellsRange->toString(), $this->options);
            /**
             * @var array<array> $values
             */
            $values = $response->getValues() ?? [];
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
}
