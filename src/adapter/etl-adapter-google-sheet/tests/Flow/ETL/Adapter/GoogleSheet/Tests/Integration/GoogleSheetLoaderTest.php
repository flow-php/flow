<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Integration;

use function Flow\ETL\Adapter\GoogleSheet\to_google_sheet;
use function Flow\ETL\DSL\{config, flow_context, int_entry, row, rows, string_entry};
use Flow\ETL\Adapter\GoogleSheet\{
    Tests\GoogleSheetsContext,
    Tests\HttpClientContext,
    Tests\HttpRequestContext};
use Flow\ETL\Tests\FlowTestCase;

final class GoogleSheetLoaderTest extends FlowTestCase
{
    public function test_load() : void
    {
        $httpContext = (new HttpClientContext())
            ->add(
                new HttpRequestContext(
                    'GET',
                    'https://sheets.googleapis.com/v4/spreadsheets/1234567890',
                ),
                __DIR__ . '/../Fixtures/get-spreadsheet-response.json'
            )
            ->add(
                new HttpRequestContext(
                    'PUT',
                    'https://sheets.googleapis.com/v4/spreadsheets/1234567890/values/%27Sheet%27%21A1%3AB4?valueInputOption=USER_ENTERED',
                    '{"majorDimension":"ROWS","values":[["id","name"],[12345,"Norbert"],[54321,"Joseph"],[666,"Dominik"]]}',
                ),
                __DIR__ . '/../Fixtures/update-spreadsheet-response.json'
            );

        $loader = to_google_sheet(
            (new GoogleSheetsContext($httpContext))
                ->sheets(),
            '1234567890',
            'Sheet',
        );

        $loader->load(
            rows(
                row(int_entry('id', 12345), string_entry('name', 'Norbert')),
                row(int_entry('id', 54321), string_entry('name', 'Joseph')),
                row(int_entry('id', 666), string_entry('name', 'Dominik'))
            ),
            flow_context(config())
        );
    }
}
