<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Rows;
use Google\Service\Sheets;
use Google\Service\Sheets\ValueRange;

final class GoogleSheetExtractor implements Extractor
{
    public function __construct(
        private readonly Sheets $service,
        private readonly string $spreadsheetId,
        private readonly Columns $columnRange,
        private readonly bool $withHeader,
        private readonly int $rowsInBatch,
        private readonly string $rowEntryName='row',
    ) {
        if ($this->rowsInBatch <= 0) {
            throw new InvalidArgumentException('Rows in batch must be greater than 0');
        }
    }

    /**
     * @psalm-suppress MixedArgument
     * @psalm-suppress MixedMethodCall
     */
    public function extract(FlowContext $context) : \Generator
    {
        $cellsRange = new SheetRange($this->columnRange, 1, $this->rowsInBatch);
        $headers = [];

        $totalRows = 0;
        /** @var ValueRange $response */
        $response = $this->service->spreadsheets_values->get($this->spreadsheetId, $cellsRange->toString());
        /** @var array[] $values */
        $values = $response->getValues();

        if ($this->withHeader && [] !== $values) {
            /** @var string[] $headers */
            $headers = $values[0];
            unset($values[0]);
            $totalRows = 1;
        }

        while ([] !== $values) {
            yield new Rows(
                ...\array_map(
                    function (array $rowData) use ($headers, $context, &$totalRows) {
                        if (\count($headers) > \count($rowData)) {
                            \array_push(
                                $rowData,
                                ...\array_map(
                                    /** @psalm-suppress UnusedClosureParam */
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

                        if ($context->config->shouldPutInputIntoRows()) {
                            return Row::create(
                                Entry::array($this->rowEntryName, \array_combine($headers, $rowData)),
                                new StringEntry('spread_sheet_id', $this->spreadsheetId),
                                new StringEntry('sheet_name', $this->columnRange->sheetName),
                            );
                        }

                        return Row::create(Entry::array($this->rowEntryName, \array_combine($headers, $rowData)));
                    },
                    $values
                )
            );

            if ($totalRows < $cellsRange->endRow) {
                return;
            }
            $cellsRange = $cellsRange->nextRows($this->rowsInBatch);
            /** @var ValueRange $response */
            $response = $this->service->spreadsheets_values->get($this->spreadsheetId, $cellsRange->toString());
            /** @var array[] $values */
            $values = $response->getValues();
        }
    }
}
