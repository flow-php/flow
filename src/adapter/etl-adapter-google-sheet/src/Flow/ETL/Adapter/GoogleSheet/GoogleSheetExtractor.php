<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Google\Service\Sheets;

final class GoogleSheetExtractor implements Extractor, LimitableExtractor
{
    use Limitable;

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
        private readonly int $rowsPerPage,
        private readonly array $options = [],
    ) {
        if ($this->rowsPerPage < 1) {
            throw new InvalidArgumentException('Rows per page must be greater than 0');
        }
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
        } else {
            $totalRows = 0;
        }

        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        while (\count($values)) {
            $rows = \array_map(
                function (array $rowData) use ($headers, $shouldPutInputIntoRows) {
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
                $signal = yield array_to_rows($row, $context->entryFactory(), schema: $context->schema());
                $this->countRow();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    return;
                }
            }

            if ($totalRows < $cellsRange->endRow) {
                return;
            }

            $cellsRange = $cellsRange->nextRows($this->rowsPerPage);
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
        }
    }
}
