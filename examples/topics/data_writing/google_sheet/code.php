<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\GoogleSheet\{from_google_sheet, google_create_spreadsheet, google_sheets, to_google_sheet};
use function Flow\ETL\DSL\{data_frame, from_array, to_stream};
use Google\Service\Sheets;
use Symfony\Component\Dotenv\Dotenv;

require __DIR__ . '/vendor/autoload.php';

if (!\file_exists(__DIR__ . '/auth.json')) {
    print 'Example skipped. Please create .env file with Google Auth credentials.' . PHP_EOL;

    return;
}

if (!\file_exists(__DIR__ . '/.env')) {
    print 'Example skipped. Please create .env file with Google Sheet details.' . PHP_EOL;

    return;
}

$dotenv = new Dotenv();
$dotenv->load(__DIR__ . '/.env');

$service = google_sheets(
    \json_decode((string) \file_get_contents(__DIR__ . '/auth.json'), true, 512, JSON_THROW_ON_ERROR),
    Sheets::SPREADSHEETS
);

$spreadsheet = google_create_spreadsheet(
    $service,
    $_ENV['GOOGLE_SPREADSHEET_NAME'],
    $sheetName = $_ENV['GOOGLE_SHEET_EMAIL'],
    $_ENV['GOOGLE_SHEET_EMAIL']
);

data_frame()
    ->read(from_array([
        ['id' => 1, 'text' => 'lorem ipsum'],
        ['id' => 2, 'text' => 'lorem ipsum'],
        ['id' => 3, 'text' => 'lorem ipsum'],
        ['id' => 4, 'text' => 'lorem ipsum'],
        ['id' => 5, 'text' => 'lorem ipsum'],
        ['id' => 6, 'text' => 'lorem ipsum'],
    ]))
    ->write(to_google_sheet($service, $spreadsheet->spreadsheetId, $sheetName))
    ->run();

data_frame()
    ->read(from_google_sheet($service, $spreadsheet->spreadsheetId, $sheetName))
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
