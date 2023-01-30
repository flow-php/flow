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
     * @param array{type: string, project_id: string, private_key_id: string, private_key: string, client_email: string, client_id: string, auth_uri: string, token_uri: string, auth_provider_x509_cert_url: string, client_x509_cert_url: string} $authConfig
     * @param string $spreadsheetId
     * @param GoogleSheetRange $initialDataRange
     * @param bool $withHeader
     * @param int $rowsInBatch
     *
     * @throws Exception
     * @throws \JsonException
     *
     * @return self
     */
    public static function create(array $authConfig, string $spreadsheetId, GoogleSheetRange $initialDataRange, bool $withHeader = true, int $rowsInBatch = 500) : self
    {
        $client = new Client();
        $client->setScopes(Sheets::SPREADSHEETS_READONLY);
        $client->setAuthConfig($authConfig);

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
