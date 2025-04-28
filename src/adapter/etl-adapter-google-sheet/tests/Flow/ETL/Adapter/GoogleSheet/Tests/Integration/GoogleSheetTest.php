<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Integration;

use function Flow\ETL\Adapter\GoogleSheet\{from_google_sheet,
    google_create_spreadsheet,
    google_sheets,
    to_google_sheet};
use function Flow\ETL\DSL\{config, flow_context, int_entry, row, rows, str_entry, string_entry};
use Flow\ETL\Tests\FlowTestCase;
use Google\Service\Sheets;
use Google\Service\Sheets\ClearValuesRequest;

final class GoogleSheetTest extends FlowTestCase
{
    private const SHEET_NAME = 'Flow Sheet';

    private Sheets $service;

    private string $spreadsheetId;

    protected function setUp() : void
    {
        parent::setUp();

        $authFilename = __DIR__ . '/../../../../../../../auth.json.dist';

        if (!file_exists($authFilename)) {
            self::markTestSkipped('auth.json.dist file is missing');
        }

        try {
            $this->service = google_sheets(
                json_decode((string) file_get_contents($authFilename), true, 512, JSON_THROW_ON_ERROR),
                Sheets::SPREADSHEETS
            );
        } catch (\JsonException) {
            self::markTestSkipped('auth.json.dist file contains not valid JSON');
        }

        $spreadsheet = google_create_spreadsheet(
            $this->service,
            'Flow test spreadsheet',
            self::SHEET_NAME,
            'random@gmail.com'
        );

        $this->spreadsheetId = $spreadsheet->spreadsheetId;

        $this->clearTestSheet();
    }

    protected function tearDown() : void
    {
        parent::tearDown();

        $this->clearTestSheet();
    }

    public function test_load_and_extract() : void
    {
        $loader = to_google_sheet(
            $this->service,
            $this->spreadsheetId,
            self::SHEET_NAME,
        );

        $loader->load(
            rows(
                row(int_entry('id', 12345), string_entry('name', 'Norbert')),
                row(int_entry('id', 54321), string_entry('name', 'Joseph')),
            ),
            $context = flow_context(config())
        );

        // Ensure previous rows are not overwritten
        $loader->load(
            rows(
                row(int_entry('id', 666), string_entry('name', 'Dominik'))
            ),
            $context
        );

        $extractor = from_google_sheet(
            $this->service,
            $this->spreadsheetId,
            self::SHEET_NAME,
        );

        $rowsArray = \iterator_to_array($extractor->extract(flow_context(config())));
        self::assertCount(3, $rowsArray);
        self::assertSame(1, $rowsArray[0]->count());
        self::assertEquals(row(string_entry('id', '12345'), string_entry('name', 'Norbert')), $rowsArray[0]->first());
        self::assertSame(1, $rowsArray[1]->count());
        self::assertEquals(row(str_entry('id', '54321'), string_entry('name', 'Joseph')), $rowsArray[1]->first());
        self::assertSame(1, $rowsArray[2]->count());
        self::assertEquals(row(str_entry('id', '666'), string_entry('name', 'Dominik')), $rowsArray[2]->first());
    }

    private function clearTestSheet() : void
    {
        $this->service->spreadsheets_values->clear(
            $this->spreadsheetId,
            "'" . self::SHEET_NAME . "'!A1:ZZ",
            new ClearValuesRequest(),
        );
    }
}
