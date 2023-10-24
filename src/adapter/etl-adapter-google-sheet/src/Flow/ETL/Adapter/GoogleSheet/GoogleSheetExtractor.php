<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Google\Service\Sheets;

final class GoogleSheetExtractor implements Extractor
{
    /**
     * @param array{dateTimeRenderOption?: string, majorDimension?: string, valueRenderOption?: string} $options
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly Sheets $service,
        private readonly string $spreadsheetId,
        private readonly Columns $columnRange,
        private readonly bool $withHeader,
        private readonly int $rowsInBatch,
        private readonly array $options = [],
    ) {
        if ($this->rowsInBatch < 1) {
            throw new InvalidArgumentException('Rows in batch must be greater than 0');
        }
    }

    public function extract(FlowContext $context) : \Generator
    {
        $cellsRange = new SheetRange($this->columnRange, 1, $this->rowsInBatch);
        $headers = [];

        $totalRows = 0;
        /** @var Sheets\ValueRange $response */
        $response = $this->service->spreadsheets_values->get($this->spreadsheetId, $cellsRange->toString(), $this->options);
        /**
         * @var array[] $values
         *
         * @psalm-suppress RedundantConditionGivenDocblockType, DocblockTypeContradiction
         *
         * @phpstan-ignore-next-line
         */
        $values = $response->getValues() ?? [];

        if ($this->withHeader && [] !== $values) {
            /** @var string[] $headers */
            $headers = $values[0];
            unset($values[0]);
            $totalRows = 1;
        }

        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        while ([] !== $values) {
            yield array_to_rows(
                \array_map(
                    function (array $rowData) use ($headers, $shouldPutInputIntoRows, &$totalRows) {
                        if (\count($headers) > \count($rowData)) {
                            \array_push(
                                $rowData,
                                ...\array_map(
                                    static fn (int $i) => null,
                                    \range(1, \count($headers) - \count($rowData))
                                )
                            );
                        }

                        if (\count($rowData) > \count($headers)) {
                            /** @phpstan-ignore-next-line */
                            $rowData = \array_chunk($rowData, \count($headers));
                        }

                        /** @var int $totalRows */
                        $totalRows++;

                        $row = \array_combine($headers, $rowData);

                        if ($shouldPutInputIntoRows) {
                            $row['_spread_sheet_id'] = $this->spreadsheetId;
                            $row['_sheet_name'] = $this->columnRange->sheetName;
                        }

                        return $row;
                    },
                    $values
                ),
                $context->entryFactory()
            );

            if ($totalRows < $cellsRange->endRow) {
                return;
            }

            $cellsRange = $cellsRange->nextRows($this->rowsInBatch);
            /** @var Sheets\ValueRange $response */
            $response = $this->service->spreadsheets_values->get($this->spreadsheetId, $cellsRange->toString(), $this->options);
            /** @var array[] $values */
            $values = $response->getValues();
        }
    }
}
