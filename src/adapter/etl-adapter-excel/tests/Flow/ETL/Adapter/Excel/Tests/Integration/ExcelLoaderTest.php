<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Integration;

use DateInterval;
use DateTimeImmutable;
use Flow\ETL\Adapter\Excel\CellStyler;
use Flow\ETL\Adapter\Excel\ExcelWriter;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Tests\FlowTestCase;
use OpenSpout\Common\Entity\Style\Style;
use OpenSpout\Writer\ODS\Options as OdsOptions;
use OpenSpout\Writer\XLSX\Options as XlsxOptions;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\Adapter\Excel\DSL\from_excel;
use function Flow\ETL\Adapter\Excel\DSL\to_excel;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\from_sequence_number;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\to_transformation;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_uuid;

final class ExcelLoaderTest extends FlowTestCase
{
    /**
     * @return iterable<string, array{ExcelWriter, string}>
     */
    public static function provide_writers(): iterable
    {
        yield 'xlsx' => [ExcelWriter::XLSX, 'xlsx'];
        yield 'ods' => [ExcelWriter::ODS, 'ods'];
    }

    public function test_auto_detects_ods_writer_from_extension(): void
    {
        $outputPath = __DIR__ . '/var/output_auto.ods';

        df()
            ->read(from_rows(rows(schema(int_schema('id'), string_schema('name')), row(['id' => 1, 'name' => 'Test']))))
            ->write(to_excel($outputPath)->saveMode(overwrite()))
            ->run();

        $rows = df()->read(from_excel($outputPath))->fetch()->toArray();

        static::assertEquals(
            [
                ['id' => 1, 'name' => 'Test'],
            ],
            $rows,
        );
    }

    public function test_auto_detects_xlsx_writer_from_extension(): void
    {
        $outputPath = __DIR__ . '/var/output_auto.xlsx';

        df()
            ->read(from_rows(rows(schema(int_schema('id'), string_schema('name')), row(['id' => 1, 'name' => 'Test']))))
            ->write(to_excel($outputPath)->saveMode(overwrite()))
            ->run();

        $rows = df()->read(from_excel($outputPath))->fetch()->toArray();

        static::assertEquals(
            [
                ['id' => 1, 'name' => 'Test'],
            ],
            $rows,
        );
    }

    public function test_multiple_rows_in_single_batch(): void
    {
        $outputPath = __DIR__ . '/var/output_batches.xlsx';

        df()
            ->read(from_rows(rows(
                schema(int_schema('id'), string_schema('name')),
                row(['id' => 1, 'name' => 'First']),
                row(['id' => 2, 'name' => 'Second']),
                row(['id' => 3, 'name' => 'Third']),
            )))
            ->batchSize(2)
            ->write(to_excel($outputPath)->saveMode(overwrite()))
            ->run();

        $rows = df()->read(from_excel($outputPath))->fetch()->toArray();

        static::assertEquals(
            [
                ['id' => 1, 'name' => 'First'],
                ['id' => 2, 'name' => 'Second'],
                ['id' => 3, 'name' => 'Third'],
            ],
            $rows,
        );
    }

    #[DataProvider('provide_writers')]
    public function test_round_trip_with_basic_data(ExcelWriter $writer, string $extension): void
    {
        $outputPath = __DIR__ . '/var/output.' . $extension;

        df()
            ->read(from_rows(rows(
                schema(int_schema('id'), string_schema('name'), string_schema('email')),
                row(['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com']),
                row(['id' => 2, 'name' => 'Bob', 'email' => 'bob@example.com']),
                row(['id' => 3, 'name' => 'Charlie', 'email' => 'charlie@example.com']),
            )))
            ->write(to_excel($outputPath)->saveMode(overwrite())->withWriter($writer))
            ->run();

        $rows = df()->read(from_excel($outputPath))->fetch()->toArray();

        static::assertEquals(
            [
                ['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com'],
                ['id' => 2, 'name' => 'Bob', 'email' => 'bob@example.com'],
                ['id' => 3, 'name' => 'Charlie', 'email' => 'charlie@example.com'],
            ],
            $rows,
        );
    }

    public function test_round_trip_with_custom_datetime_and_time_formats(): void
    {
        $outputPath = __DIR__ . '/var/output_custom_formats.xlsx';
        $date = new DateTimeImmutable('2024-06-15');
        $datetime = new DateTimeImmutable('2024-06-15 14:30:45');
        $time = new DateInterval('PT14H30M45S');

        df()
            ->read(from_rows(rows(
                schema(date_schema('date_val'), datetime_schema('datetime_val'), time_schema('time_val')),
                row(['date_val' => $date, 'datetime_val' => $datetime, 'time_val' => $time]),
            )))
            ->write(
                to_excel($outputPath)->saveMode(overwrite())->withDateTimeFormat('d/m/Y H:i')->withTimeFormat('%H:%I'),
            )
            ->run();

        $rows = df()->read(from_excel($outputPath))->fetch()->toArray();

        static::assertEquals(
            [
                ['date_val' => '2024-06-15', 'datetime_val' => '15/06/2024 14:30', 'time_val' => '14:30'],
            ],
            $rows,
        );
    }

    #[DataProvider('provide_writers')]
    public function test_round_trip_with_custom_sheet_name(ExcelWriter $writer, string $extension): void
    {
        $outputPath = __DIR__ . '/var/output_custom_sheet.' . $extension;

        df()
            ->read(from_rows(rows(schema(int_schema('id'), string_schema('name')), row(['id' => 1, 'name' => 'Test']))))
            ->write(to_excel($outputPath)->saveMode(overwrite())->withSheetName('MySheet')->withWriter($writer))
            ->run();

        $rows = df()->read(from_excel($outputPath)->withSheetName('MySheet'))->fetch()->toArray();

        static::assertEquals(
            [
                ['id' => 1, 'name' => 'Test'],
            ],
            $rows,
        );
    }

    public function test_round_trip_with_datetime_xlsx(): void
    {
        $outputPath = __DIR__ . '/var/output_datetime.xlsx';
        $date = new DateTimeImmutable('2024-06-15');
        $datetime = new DateTimeImmutable('2024-06-15 14:30:00');

        df()
            ->read(from_rows(rows(
                schema(date_schema('date_val'), datetime_schema('datetime_val')),
                row(['date_val' => $date, 'datetime_val' => $datetime]),
            )))
            ->write(to_excel($outputPath)->saveMode(overwrite())->withWriter(ExcelWriter::XLSX))
            ->run();

        $rows = df()->read(from_excel($outputPath))->fetch()->toArray();

        static::assertEquals(
            [
                ['date_val' => '2024-06-15', 'datetime_val' => '2024-06-15 14:30:00'],
            ],
            $rows,
        );
    }

    public function test_round_trip_with_dynamic_sheet_names(): void
    {
        $outputPath = __DIR__ . '/var/output_dynamic_sheets.xlsx';

        $loader = to_excel($outputPath);
        $loader = $loader->withSheetNameFromEntry('category');

        df()
            ->read(from_rows(rows(
                schema(int_schema('id'), string_schema('name'), string_schema('category')),
                row(['id' => 1, 'name' => 'Alice', 'category' => 'Users']),
                row(['id' => 2, 'name' => 'Bob', 'category' => 'Users']),
                row(['id' => 3, 'name' => 'Product A', 'category' => 'Products']),
            )))
            ->write($loader->saveMode(overwrite()))
            ->run();

        $usersRows = df()->read(from_excel($outputPath)->withSheetName('Users'))->fetch()->toArray();

        static::assertEquals(
            [
                ['id' => 1, 'name' => 'Alice'],
                ['id' => 2, 'name' => 'Bob'],
            ],
            $usersRows,
        );

        $productsRows = df()->read(from_excel($outputPath)->withSheetName('Products'))->fetch()->toArray();

        static::assertEquals(
            [
                ['id' => 3, 'name' => 'Product A'],
            ],
            $productsRows,
        );
    }

    #[DataProvider('provide_writers')]
    public function test_round_trip_with_null_values(ExcelWriter $writer, string $extension): void
    {
        $outputPath = __DIR__ . '/var/output_nulls.' . $extension;

        df()
            ->read(from_rows(rows(
                schema(int_schema('id'), string_schema('name', nullable: true), string_schema('email', nullable: true)),
                row(['id' => 1, 'name' => 'Alice', 'email' => null]),
                row(['id' => 2, 'name' => null, 'email' => 'bob@example.com']),
            )))
            ->write(to_excel($outputPath)->saveMode(overwrite())->withWriter($writer))
            ->run();

        $rows = df()->read(from_excel($outputPath))->fetch()->toArray();

        static::assertEquals(
            [
                ['id' => 1, 'name' => 'Alice', 'email' => null],
                ['id' => 2, 'name' => null, 'email' => 'bob@example.com'],
            ],
            $rows,
        );
    }

    #[DataProvider('provide_writers')]
    public function test_round_trip_with_various_data_types(ExcelWriter $writer, string $extension): void
    {
        $outputPath = __DIR__ . '/var/output_types.' . $extension;
        $uuidString = 'f47ac10b-58cc-4372-a567-0e02b2c3d479';

        df()
            ->read(from_rows(rows(
                schema(
                    int_schema('int_val'),
                    float_schema('float_val'),
                    bool_schema('bool_val'),
                    string_schema('string_val'),
                    uuid_schema('uuid_val'),
                    json_schema('json_val'),
                ),
                row([
                    'int_val' => 42,
                    'float_val' => 3.14,
                    'bool_val' => true,
                    'string_val' => 'hello',
                    'uuid_val' => type_uuid()->cast($uuidString),
                    'json_val' => type_json()->cast(['key' => 'value']),
                ]),
            )))
            ->write(to_excel($outputPath)->saveMode(overwrite())->withWriter($writer))
            ->run();

        $rows = df()->read(from_excel($outputPath))->fetch()->toArray();

        static::assertEquals(
            [
                [
                    'int_val' => 42,
                    'float_val' => 3.14,
                    'bool_val' => true,
                    'string_val' => 'hello',
                    'uuid_val' => $uuidString,
                    'json_val' => '{"key":"value"}',
                ],
            ],
            $rows,
        );
    }

    #[DataProvider('provide_writers')]
    public function test_round_trip_without_header(ExcelWriter $writer, string $extension): void
    {
        $outputPath = __DIR__ . '/var/output_no_header.' . $extension;

        df()
            ->read(from_rows(rows(
                schema(int_schema('id'), string_schema('name')),
                row(['id' => 1, 'name' => 'Alice']),
                row(['id' => 2, 'name' => 'Bob']),
            )))
            ->write(to_excel($outputPath)->saveMode(overwrite())->withHeader(false)->withWriter($writer))
            ->run();

        $rows = df()->read(from_excel($outputPath)->withHeader(false))->fetch()->toArray();

        static::assertEquals(
            [
                ['e00' => 1, 'e01' => 'Alice'],
                ['e00' => 2, 'e01' => 'Bob'],
            ],
            $rows,
        );
    }

    public function test_sheet_name_and_sheet_name_from_entry_are_mutually_exclusive(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cannot set both sheetName and sheetNameFromEntry');

        to_excel('/tmp/test.xlsx')->withSheetName('MySheet')->withSheetNameFromEntry('category');
    }

    public function test_sheet_name_from_entry_and_sheet_name_are_mutually_exclusive(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cannot set both sheetName and sheetNameFromEntry');

        to_excel('/tmp/test.xlsx')->withSheetNameFromEntry('category')->withSheetName('MySheet');
    }

    public function test_transformation_loader_writes_all_batches_to_excel(): void
    {
        $outputPath = __DIR__ . '/var/output_transformation.xlsx';

        df()
            ->read(from_sequence_number('id', 1, 12))
            ->withEntry('name', lit('dropped by the transformation'))
            ->batchSize(4)
            ->write(to_transformation(select('id'), to_excel($outputPath)->saveMode(overwrite())))
            ->run();

        $rows = df()->read(from_excel($outputPath))->fetch();

        static::assertCount(12, $rows);
        static::assertSame(1, $rows->schema()->count());
    }

    public function test_with_cell_styler(): void
    {
        $outputPath = __DIR__ . '/var/output_cell_styler.xlsx';

        $cellStyler = new class implements CellStyler {
            public function style(
                mixed $value,
                Definition $definition,
                int $rowNumber,
                int $columnIndex,
                string $sheetName,
            ): ?Style {
                if ($columnIndex === 0) {
                    return new Style(fontBold: true);
                }

                return null;
            }
        };

        $loader = to_excel($outputPath);
        $loader = $loader->withCellStyler($cellStyler);

        df()
            ->read(from_rows(rows(
                schema(int_schema('id'), string_schema('name')),
                row(['id' => 1, 'name' => 'Alice']),
                row(['id' => 2, 'name' => 'Bob']),
            )))
            ->write($loader->saveMode(overwrite()))
            ->run();

        $rows = df()->read(from_excel($outputPath))->fetch()->toArray();

        static::assertEquals(
            [
                ['id' => 1, 'name' => 'Alice'],
                ['id' => 2, 'name' => 'Bob'],
            ],
            $rows,
        );
    }

    public function test_with_header_style(): void
    {
        $outputPath = __DIR__ . '/var/output_header_style.xlsx';

        $headerStyle = new Style(fontBold: true);

        $loader = to_excel($outputPath);
        $loader = $loader->withHeaderStyle($headerStyle);

        df()
            ->read(from_rows(rows(schema(int_schema('id'), string_schema('name')), row(['id' => 1, 'name' => 'Test']))))
            ->write($loader->saveMode(overwrite()))
            ->run();

        $rows = df()->read(from_excel($outputPath))->fetch()->toArray();

        static::assertEquals(
            [
                ['id' => 1, 'name' => 'Test'],
            ],
            $rows,
        );
    }

    public function test_with_ods_options(): void
    {
        $outputPath = __DIR__ . '/var/output_with_ods_options.ods';

        $options = new OdsOptions(DEFAULT_COLUMN_WIDTH: 15.0, DEFAULT_ROW_HEIGHT: 20.0);

        df()
            ->read(from_rows(rows(schema(int_schema('id'), string_schema('name')), row(['id' => 1, 'name' => 'Test']))))
            ->write(to_excel($outputPath)->saveMode(overwrite())->withWriterOptions($options))
            ->run();

        $rows = df()->read(from_excel($outputPath))->fetch()->toArray();

        static::assertEquals(
            [
                ['id' => 1, 'name' => 'Test'],
            ],
            $rows,
        );
    }

    public function test_with_xlsx_options(): void
    {
        $outputPath = __DIR__ . '/var/output_with_options.xlsx';

        $options = new XlsxOptions(SHOULD_USE_INLINE_STRINGS: false, DEFAULT_COLUMN_WIDTH: 15.0);

        df()
            ->read(from_rows(rows(schema(int_schema('id'), string_schema('name')), row(['id' => 1, 'name' => 'Test']))))
            ->write(to_excel($outputPath)->saveMode(overwrite())->withWriterOptions($options))
            ->run();

        $rows = df()->read(from_excel($outputPath))->fetch()->toArray();

        static::assertEquals(
            [
                ['id' => 1, 'name' => 'Test'],
            ],
            $rows,
        );
    }
}
