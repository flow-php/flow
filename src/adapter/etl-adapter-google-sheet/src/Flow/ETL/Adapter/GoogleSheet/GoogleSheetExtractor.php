<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Google\Client;
use Google\Exception;
use Google\Service\Sheets;
use Webmozart\Assert\Assert;

final class GoogleSheetExtractor implements Extractor
{
    public function __construct(
        private readonly Sheets $service,
        private readonly string $spreadsheetId,
        private readonly GoogleSheetRange $initialDataRange,
        private readonly bool $withHeader,
        private readonly int $rowsInBatch,
    ) {
        Assert::greaterThan($rowsInBatch, 0);
    }

    /**
     * @throws Exception
     * @throws \JsonException
     */
    public static function create(string $authJson, string $spreadsheetId, GoogleSheetRange $initialDataRange, bool $withHeader = true, int $rowsInBatch = 500) : self
    {
        $client = new Client();
        $client->setScopes(Sheets::SPREADSHEETS_READONLY);
        $client->setAuthConfig(\json_decode($authJson, true, 512, JSON_THROW_ON_ERROR));

        return new self(new Sheets($client), $spreadsheetId, $initialDataRange, $withHeader, $rowsInBatch);
    }

    public function extract(FlowContext $context) : \Generator
    {
        $range = $this->initialDataRange;
        $headers = [];

        $totalRows = 0;

        $response = $this->service->spreadsheets_values->get($this->spreadsheetId, $range->toString());
        $values = $response->getValues();

        if ($this->withHeader && \count($values) > 0) {
            $headers = $values[0];
            unset($values[0]);
        }

        while (\is_array($values) && \count($values) > 0) {
            yield new Rows(
                ...\array_map(
                    static function ($rowData) use ($headers, &$totalRows) {
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

                        return Row::create(Entry::array('row', \array_combine($headers, $rowData)));
                    },
                    $values
                )
            );

            if ($totalRows < $range->endRow) {
                return;
            }
            $range = $range->nextRowsRange($this->rowsInBatch);
            $response = $this->service->spreadsheets_values->get($this->spreadsheetId, $range->toString());
            $values = $response->getValues();
        }
    }
}
