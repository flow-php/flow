<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Integration;

use function Flow\ETL\Adapter\Excel\DSL\{from_excel, to_excel};
use function Flow\ETL\DSL\{bool_entry, date_entry, datetime_entry, df, float_entry, from_rows, int_entry, json_entry, row, rows, string_entry, time_entry, uuid_entry};
use Flow\ETL\Adapter\Excel\{CellStyler, ExcelWriter};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use OpenSpout\Common\Entity\Style\Style;
use OpenSpout\Writer\ODS\Options as OdsOptions;
use OpenSpout\Writer\XLSX\Options as XlsxOptions;
use PHPUnit\Framework\Attributes\DataProvider;

final class ExcelLoaderTest extends FlowIntegrationTestCase
{
    /**
     * @return iterable<string, array{ExcelWriter, string}>
     */
    public static function provide_writers() : iterable
    {
        yield 'xlsx' => [ExcelWriter::XLSX, 'xlsx'];
        yield 'ods' => [ExcelWriter::ODS, 'ods'];
    }

    public function test_auto_detects_ods_writer_from_extension() : void
    {
        $outputPath = $this->cacheDir->suffix('output_auto.ods')->path();

        df()
            ->read(from_rows(
                rows(
                    row(int_entry('id', 1), string_entry('name', 'Test')),
                )
            ))
            ->write(to_excel($outputPath))
            ->run();

        $rows = df()
            ->read(from_excel($outputPath))
            ->fetch()
            ->toArray();

        self::assertCount(1, $rows);
        self::assertSame([1, 'Test'], [$rows[0]['id'], $rows[0]['name']]);
    }

    public function test_auto_detects_xlsx_writer_from_extension() : void
    {
        $outputPath = $this->cacheDir->suffix('output_auto.xlsx')->path();

        df()
            ->read(from_rows(
                rows(
                    row(int_entry('id', 1), string_entry('name', 'Test')),
                )
            ))
            ->write(to_excel($outputPath))
            ->run();

        $rows = df()
            ->read(from_excel($outputPath))
            ->fetch()
            ->toArray();

        self::assertCount(1, $rows);
        self::assertSame([1, 'Test'], [$rows[0]['id'], $rows[0]['name']]);
    }

    public function test_destination_returns_path() : void
    {
        $loader = to_excel('/tmp/test.xlsx');

        self::assertSame('/tmp/test.xlsx', $loader->destination()->path());
    }

    public function test_multiple_rows_in_single_batch() : void
    {
        $outputPath = $this->cacheDir->suffix('output_batches.xlsx')->path();

        df()
            ->read(from_rows(
                rows(
                    row(int_entry('id', 1), string_entry('name', 'First')),
                    row(int_entry('id', 2), string_entry('name', 'Second')),
                    row(int_entry('id', 3), string_entry('name', 'Third')),
                )
            ))
            ->batchSize(2)
            ->write(to_excel($outputPath))
            ->run();

        $rows = df()
            ->read(from_excel($outputPath))
            ->fetch()
            ->toArray();

        self::assertCount(3, $rows);
    }

    public function test_non_local_path_throws_exception() : void
    {
        $this->expectException(\Flow\ETL\Exception\InvalidArgumentException::class);
        $this->expectExceptionMessage('Only local filesystem paths are supported');

        to_excel('s3://bucket/path/to/file.xlsx');
    }

    #[DataProvider('provide_writers')]
    public function test_round_trip_with_basic_data(ExcelWriter $writer, string $extension) : void
    {
        $outputPath = $this->cacheDir->suffix('output.' . $extension)->path();

        df()
            ->read(from_rows(
                rows(
                    row(int_entry('id', 1), string_entry('name', 'Alice'), string_entry('email', 'alice@example.com')),
                    row(int_entry('id', 2), string_entry('name', 'Bob'), string_entry('email', 'bob@example.com')),
                    row(int_entry('id', 3), string_entry('name', 'Charlie'), string_entry('email', 'charlie@example.com')),
                )
            ))
            ->write(to_excel($outputPath)->withWriter($writer))
            ->run();

        $rows = df()
            ->read(from_excel($outputPath))
            ->fetch()
            ->toArray();

        self::assertCount(3, $rows);
        self::assertSame(['id', 'name', 'email'], \array_keys($rows[0]));
        self::assertSame([1, 'Alice', 'alice@example.com'], [$rows[0]['id'], $rows[0]['name'], $rows[0]['email']]);
        self::assertSame([2, 'Bob', 'bob@example.com'], [$rows[1]['id'], $rows[1]['name'], $rows[1]['email']]);
        self::assertSame([3, 'Charlie', 'charlie@example.com'], [$rows[2]['id'], $rows[2]['name'], $rows[2]['email']]);
    }

    public function test_round_trip_with_custom_date_formats() : void
    {
        $outputPath = $this->cacheDir->suffix('output_custom_formats.xlsx')->path();
        $date = new \DateTimeImmutable('2024-06-15');
        $datetime = new \DateTimeImmutable('2024-06-15 14:30:45');
        $time = new \DateInterval('PT14H30M45S');

        df()
            ->read(from_rows(
                rows(
                    row(
                        date_entry('date_val', $date),
                        datetime_entry('datetime_val', $datetime),
                        time_entry('time_val', $time),
                    ),
                )
            ))
            ->write(
                to_excel($outputPath)
                    ->withDateFormat('d/m/Y')
                    ->withDateTimeFormat('d/m/Y H:i')
                    ->withTimeFormat('%H:%I')
            )
            ->run();

        $rows = df()
            ->read(from_excel($outputPath))
            ->fetch()
            ->toArray();

        self::assertCount(1, $rows);
        self::assertSame('15/06/2024', $rows[0]['date_val']);
        self::assertSame('15/06/2024 14:30', $rows[0]['datetime_val']);
        self::assertSame('14:30', $rows[0]['time_val']);
    }

    #[DataProvider('provide_writers')]
    public function test_round_trip_with_custom_sheet_name(ExcelWriter $writer, string $extension) : void
    {
        $outputPath = $this->cacheDir->suffix('output_custom_sheet.' . $extension)->path();

        df()
            ->read(from_rows(
                rows(
                    row(int_entry('id', 1), string_entry('name', 'Test')),
                )
            ))
            ->write(
                to_excel($outputPath)->withSheetName('MySheet')->withWriter($writer)
            )
            ->run();

        $rows = df()
            ->read(from_excel($outputPath)->withSheetName('MySheet'))
            ->fetch()
            ->toArray();

        self::assertCount(1, $rows);
        self::assertSame([1, 'Test'], [$rows[0]['id'], $rows[0]['name']]);
    }

    public function test_round_trip_with_datetime_xlsx() : void
    {
        $outputPath = $this->cacheDir->suffix('output_datetime.xlsx')->path();
        $date = new \DateTimeImmutable('2024-06-15');
        $datetime = new \DateTimeImmutable('2024-06-15 14:30:00');

        df()
            ->read(from_rows(
                rows(
                    row(
                        date_entry('date_val', $date),
                        datetime_entry('datetime_val', $datetime),
                    ),
                )
            ))
            ->write(to_excel($outputPath)->withWriter(ExcelWriter::XLSX))
            ->run();

        $rows = df()
            ->read(from_excel($outputPath))
            ->fetch()
            ->toArray();

        self::assertCount(1, $rows);
        self::assertNotNull($rows[0]['date_val']);
        self::assertNotNull($rows[0]['datetime_val']);
    }

    public function test_round_trip_with_dynamic_sheet_names() : void
    {
        $outputPath = $this->cacheDir->suffix('output_dynamic_sheets.xlsx')->path();

        $loader = to_excel($outputPath);
        $loader = $loader->withSheetNameFromEntry('category');

        df()
            ->read(from_rows(
                rows(
                    row(int_entry('id', 1), string_entry('name', 'Alice'), string_entry('category', 'Users')),
                    row(int_entry('id', 2), string_entry('name', 'Bob'), string_entry('category', 'Users')),
                    row(int_entry('id', 3), string_entry('name', 'Product A'), string_entry('category', 'Products')),
                )
            ))
            ->write($loader)
            ->run();

        $usersRows = df()
            ->read(from_excel($outputPath)->withSheetName('Users'))
            ->fetch()
            ->toArray();

        self::assertCount(2, $usersRows);
        self::assertSame(['id', 'name'], \array_keys($usersRows[0]));

        $productsRows = df()
            ->read(from_excel($outputPath)->withSheetName('Products'))
            ->fetch()
            ->toArray();

        self::assertCount(1, $productsRows);
        self::assertSame(['id', 'name'], \array_keys($productsRows[0]));
    }

    #[DataProvider('provide_writers')]
    public function test_round_trip_with_null_values(ExcelWriter $writer, string $extension) : void
    {
        $outputPath = $this->cacheDir->suffix('output_nulls.' . $extension)->path();

        df()
            ->read(from_rows(
                rows(
                    row(int_entry('id', 1), string_entry('name', 'Alice'), string_entry('email', null)),
                    row(int_entry('id', 2), string_entry('name', null), string_entry('email', 'bob@example.com')),
                )
            ))
            ->write(to_excel($outputPath)->withWriter($writer))
            ->run();

        $rows = df()
            ->read(from_excel($outputPath))
            ->fetch()
            ->toArray();

        self::assertCount(2, $rows);
        self::assertNull($rows[0]['email']);
        self::assertNull($rows[1]['name']);
    }

    #[DataProvider('provide_writers')]
    public function test_round_trip_with_various_data_types(ExcelWriter $writer, string $extension) : void
    {
        $outputPath = $this->cacheDir->suffix('output_types.' . $extension)->path();
        $uuidString = 'f47ac10b-58cc-4372-a567-0e02b2c3d479';

        df()
            ->read(from_rows(
                rows(
                    row(
                        int_entry('int_val', 42),
                        float_entry('float_val', 3.14),
                        bool_entry('bool_val', true),
                        string_entry('string_val', 'hello'),
                        uuid_entry('uuid_val', $uuidString),
                        json_entry('json_val', ['key' => 'value']),
                    ),
                )
            ))
            ->write(to_excel($outputPath)->withWriter($writer))
            ->run();

        $rows = df()
            ->read(from_excel($outputPath))
            ->fetch()
            ->toArray();

        self::assertCount(1, $rows);
        self::assertSame(42, $rows[0]['int_val']);
        self::assertEqualsWithDelta(3.14, $rows[0]['float_val'], 0.0001);
        self::assertTrue($rows[0]['bool_val']);
        self::assertSame('hello', $rows[0]['string_val']);
        self::assertSame($uuidString, $rows[0]['uuid_val']);
        self::assertSame('{"key":"value"}', $rows[0]['json_val']);
    }

    #[DataProvider('provide_writers')]
    public function test_round_trip_without_header(ExcelWriter $writer, string $extension) : void
    {
        $outputPath = $this->cacheDir->suffix('output_no_header.' . $extension)->path();

        df()
            ->read(from_rows(
                rows(
                    row(int_entry('id', 1), string_entry('name', 'Alice')),
                    row(int_entry('id', 2), string_entry('name', 'Bob')),
                )
            ))
            ->write(to_excel($outputPath)->withHeader(false)->withWriter($writer))
            ->run();

        $rows = df()
            ->read(from_excel($outputPath)->withHeader(false))
            ->fetch()
            ->toArray();

        self::assertGreaterThanOrEqual(1, \count($rows));
        self::assertSame(['e00', 'e01'], \array_keys($rows[0]));
    }

    public function test_sheet_name_and_sheet_name_from_entry_are_mutually_exclusive() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cannot set both sheetName and sheetNameFromEntry');

        to_excel('/tmp/test.xlsx')
            ->withSheetName('MySheet')
            ->withSheetNameFromEntry('category');
    }

    public function test_sheet_name_from_entry_and_sheet_name_are_mutually_exclusive() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cannot set both sheetName and sheetNameFromEntry');

        to_excel('/tmp/test.xlsx')
            ->withSheetNameFromEntry('category')
            ->withSheetName('MySheet');
    }

    public function test_with_cell_styler() : void
    {
        $outputPath = $this->cacheDir->suffix('output_cell_styler.xlsx')->path();

        $cellStyler = new class implements CellStyler {
            public function style(Entry $entry, int $rowNumber, int $columnIndex, string $sheetName) : ?Style
            {
                if ($columnIndex === 0) {
                    return new Style(fontBold: true);
                }

                return null;
            }
        };

        $loader = to_excel($outputPath);
        $loader = $loader->withCellStyler($cellStyler);

        df()
            ->read(from_rows(
                rows(
                    row(int_entry('id', 1), string_entry('name', 'Alice')),
                    row(int_entry('id', 2), string_entry('name', 'Bob')),
                )
            ))
            ->write($loader)
            ->run();

        $rows = df()
            ->read(from_excel($outputPath))
            ->fetch()
            ->toArray();

        self::assertCount(2, $rows);
    }

    public function test_with_header_style() : void
    {
        $outputPath = $this->cacheDir->suffix('output_header_style.xlsx')->path();

        $headerStyle = (new Style(fontBold: true));

        $loader = to_excel($outputPath);
        $loader = $loader->withHeaderStyle($headerStyle);

        df()
            ->read(from_rows(
                rows(
                    row(int_entry('id', 1), string_entry('name', 'Test')),
                )
            ))
            ->write($loader)
            ->run();

        $rows = df()
            ->read(from_excel($outputPath))
            ->fetch()
            ->toArray();

        self::assertCount(1, $rows);
        self::assertSame([1, 'Test'], [$rows[0]['id'], $rows[0]['name']]);
    }

    public function test_with_ods_options() : void
    {
        $outputPath = $this->cacheDir->suffix('output_with_ods_options.ods')->path();

        $options = new OdsOptions(
            DEFAULT_COLUMN_WIDTH: 15.0,
            DEFAULT_ROW_HEIGHT: 20.0,
        );

        df()
            ->read(from_rows(
                rows(
                    row(int_entry('id', 1), string_entry('name', 'Test')),
                )
            ))
            ->write(to_excel($outputPath)->withWriterOptions($options))
            ->run();

        $rows = df()
            ->read(from_excel($outputPath))
            ->fetch()
            ->toArray();

        self::assertCount(1, $rows);
        self::assertSame([1, 'Test'], [$rows[0]['id'], $rows[0]['name']]);
    }

    public function test_with_xlsx_options() : void
    {
        $outputPath = $this->cacheDir->suffix('output_with_options.xlsx')->path();

        $options = new XlsxOptions(
            SHOULD_USE_INLINE_STRINGS: false,
            DEFAULT_COLUMN_WIDTH: 15.0,
        );

        df()
            ->read(from_rows(
                rows(
                    row(int_entry('id', 1), string_entry('name', 'Test')),
                )
            ))
            ->write(to_excel($outputPath)->withWriterOptions($options))
            ->run();

        $rows = df()
            ->read(from_excel($outputPath))
            ->fetch()
            ->toArray();

        self::assertCount(1, $rows);
        self::assertSame([1, 'Test'], [$rows[0]['id'], $rows[0]['name']]);
    }
}
