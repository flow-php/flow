<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Google\Service\Sheets;
use Webmozart\Assert\Assert;

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
        Assert::greaterThan($rowsInBatch, 0);
    }

    public function extract(FlowContext $context) : \Generator
    {
        $cellsRange = new SheetRange($this->columnRange, 1, $this->rowsInBatch);
        $headers = [];

        $totalRows = 0;

        $response = $this->service->spreadsheets_values->get($this->spreadsheetId, $cellsRange->toString());
        $values = $response->getValues();

        if ($this->withHeader && \count($values) > 0) {
            $headers = $values[0];
            unset($values[0]);
            $totalRows=1;
        }

        while (\is_array($values) && \count($values) > 0) {
            yield new Rows(
                ...\array_map(
                    function ($rowData) use ($headers, &$totalRows) {
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
                        $totalRows++;

                        return Row::create(Entry::array($this->rowEntryName, \array_combine($headers, $rowData)));
                    },
                    $values
                )
            );

            if ($totalRows < $cellsRange->endRow) {
                return;
            }
            $cellsRange = $cellsRange->nextRows($this->rowsInBatch);
            $response = $this->service->spreadsheets_values->get($this->spreadsheetId, $cellsRange->toString());
            $values = $response->getValues();
        }
    }
}
