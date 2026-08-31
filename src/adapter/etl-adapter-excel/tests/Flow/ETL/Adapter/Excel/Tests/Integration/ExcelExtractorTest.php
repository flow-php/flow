<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Integration;

use Flow\ETL\Adapter\Excel\ExcelReader;
use Flow\ETL\Config;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_keys;
use function Flow\ETL\Adapter\Excel\DSL\from_excel;
use function Flow\ETL\Adapter\Excel\DSL\is_valid_excel_sheet_name;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\Filesystem\DSL\path_real;

final class ExcelExtractorTest extends FlowTestCase
{
    /**
     * @return iterable<string, array<string>>
     */
    public static function provide_fixtures(): iterable
    {
        yield 'ods' => [__DIR__ . '/../Fixtures/fixture.ods'];
        yield 'xlsx' => [__DIR__ . '/../Fixtures/fixture.xlsx'];
        yield 'alike ods' => [__DIR__ . '/../Fixtures/fixture_as_ods'];
        yield 'alike xlsx' => [__DIR__ . '/../Fixtures/fixture_as_xlsx'];
    }

    /**
     * @return iterable<string, array<string>>
     */
    public static function provide_nullable_fixtures(): iterable
    {
        yield 'ods' => [__DIR__ . '/../Fixtures/nullable_fixture.ods'];
        yield 'xlsx' => [__DIR__ . '/../Fixtures/nullable_fixture.xlsx'];
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file(string $fixtureName): void
    {
        $rows = df()->extract(from_excel($fixtureName))->fetch()->toArray();

        static::assertCount(10, $rows);
        static::assertNull($rows[9]['email']);
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_empty_cells(string $fixtureName): void
    {
        $rows = df()->extract(from_excel($fixtureName)->withConvertEmptyToNull(false))->fetch()->toArray();

        static::assertCount(10, $rows);
        static::assertEmpty($rows[9]['email']);
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_limit(string $fixtureName): void
    {
        $extractor = from_excel($fixtureName);
        $extractor->changeLimit(5);

        $rows = df()->extract($extractor)->fetch()->toArray();

        static::assertCount(5, $rows);

        foreach ($rows as $row) {
            static::assertSame(['id', 'name', 'email'], array_keys($row));
            static::assertCount(3, $row);
        }
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_offset(string $fixtureName): void
    {
        $extractor = from_excel($fixtureName);
        $extractor->withOffset(5);

        $rows = df()->extract($extractor)->fetch()->toArray();

        static::assertCount(7, $rows);

        foreach ($rows as $row) {
            static::assertSame(['id', 'name', 'email'], array_keys($row));
            static::assertCount(3, $row);
        }
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_offset_without_header(string $fixtureName): void
    {
        $extractor = from_excel($fixtureName);
        $extractor->withHeader(false);
        $extractor->withOffset(5);

        $rows = df()->extract($extractor)->fetch()->toArray();

        static::assertCount(7, $rows);

        foreach ($rows as $row) {
            static::assertSame(['e00', 'e01', 'e02'], array_keys($row));
            static::assertCount(3, $row);
        }
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_selected_sheet_name(string $fixtureName): void
    {
        $rows = df()->extract(from_excel($fixtureName)->withSheetName('Sheet2'))->fetch()->toArray();

        static::assertCount(5, $rows);

        foreach ($rows as $row) {
            static::assertSame(['id', 'name', 'email'], array_keys($row));
            static::assertCount(3, $row);
        }
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_unknown_sheet_name(string $fixtureName): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Sheet with name: 'unknown' not found.");

        df()->extract(from_excel($fixtureName)->withSheetName('unknown'))->fetch()->toArray();
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_without_header(string $fixtureName): void
    {
        $rows = df()->extract(from_excel($fixtureName)->withHeader(false))->fetch()->toArray();

        static::assertCount(11, $rows);

        foreach ($rows as $row) {
            static::assertSame(['e00', 'e01', 'e02'], array_keys($row));
            static::assertCount(3, $row);
        }
    }

    #[DataProvider('provide_nullable_fixtures')]
    public function test_extract_excel_nullable_file(string $fixtureName): void
    {
        $rows = df()->extract(from_excel($fixtureName))->fetch()->toArray();

        static::assertCount(5, $rows);

        foreach ($rows as $row) {
            static::assertSame(['id', 'name', 'email'], array_keys($row));
            static::assertCount(3, $row);
        }
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_puts_null_in_not_matching_schema_rows(string $fixtureName): void
    {
        $rows = df()
            ->extract(from_excel($fixtureName)->withSchema(schema(
                int_schema('id'),
                string_schema('name'),
                string_schema('email'),
                string_schema('missing'),
            )))
            ->fetch()
            ->toArray();

        foreach ($rows as $row) {
            static::assertNotSame([], $row);
            static::assertNull($row['missing']);
        }
    }

    public function test_extract_with_explicit_ods_reader(): void
    {
        $rows = df()
            ->extract(from_excel(__DIR__ . '/../Fixtures/fixture.ods')->withReader(ExcelReader::ODS))
            ->fetch()
            ->toArray();

        static::assertCount(10, $rows);
        static::assertSame(['id', 'name', 'email'], array_keys($rows[0]));
    }

    public function test_extract_with_explicit_xlsx_reader(): void
    {
        $rows = df()
            ->extract(from_excel(__DIR__ . '/../Fixtures/fixture.xlsx')->withReader(ExcelReader::XLSX))
            ->fetch()
            ->toArray();

        static::assertCount(10, $rows);
        static::assertSame(['id', 'name', 'email'], array_keys($rows[0]));
    }

    public function test_extract_with_unknown_file(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unsupported file format: n/a');

        df()
            ->extract(from_excel(__DIR__ . '/../Fixtures/empty_file'))
            ->fetch()
            ->toArray();
    }

    public function test_extract_with_wrongly_selected_reader(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Failed to open file: Could not open');

        df()
            ->extract(from_excel(__DIR__ . '/../Fixtures/fixture.xlsx')->withReader(ExcelReader::ODS))
            ->fetch()
            ->toArray();
    }

    public function test_is_valid_excel_sheet_name_function(): void
    {
        $result = df()
            ->read(from_rows(rows(
                schema(string_schema('sheet')),
                row(['sheet' => 'ValidSheet']),
                row(['sheet' => 'Invalid/Sheet']),
                row(['sheet' => 'Sheet*Name']),
                row(['sheet' => 'This is a very long sheet name that exceeds the 31 character limit']),
                row(['sheet' => 'Normal']),
            )))
            ->withEntry('is_valid', is_valid_excel_sheet_name(ref('sheet')))
            ->fetch()
            ->toArray();

        static::assertTrue($result[0]['is_valid']);
        static::assertFalse($result[1]['is_valid']);
        static::assertFalse($result[2]['is_valid']);
        static::assertFalse($result[3]['is_valid']);
        static::assertTrue($result[4]['is_valid']);
    }

    public function test_extract_does_not_mutate_user_provided_schema(): void
    {
        $schema = schema(string_schema('group'), int_schema('id'), string_schema('value'));

        $extractor = from_excel(__DIR__ . '/../Fixtures/cross_stream/*/*.xlsx')
            ->withSchema($schema)
            ->withMetadataColumns(true);

        df(Config::builder())->read($extractor)->run();
        df(Config::builder())->read($extractor)->run();

        static::assertNull($schema->findDefinition('date'));
        static::assertNull($schema->findDefinition('_input_file_uri'));
    }

    public function test_loading_data_from_all_partitions(): void
    {
        df()->read(from_excel(__DIR__ . '/../Fixtures/partitioned/group=*/*.xlsx'))->run(function (Rows $rows): void {
            // the partition column comes back as a column, discovered from the path
            $this->assertContains('group', $rows->schema()->references()->names());
        });
    }

    public function test_partition_columns_are_not_leaking_between_streams(): void
    {
        static::assertSame(
            [
                ['group' => '1', 'id' => 1, 'value' => 'a', 'date' => '2026-01-01'],
                ['group' => '1', 'id' => 2, 'value' => 'b', 'date' => '2026-01-01'],
                ['group' => '2', 'id' => 5, 'value' => 'e', 'date' => null],
                ['group' => '2', 'id' => 6, 'value' => 'f', 'date' => null],
            ],
            df()
                ->read(from_excel(__DIR__ . '/../Fixtures/cross_stream/*/*.xlsx')->withSchema(schema(
                    string_schema('group'),
                    int_schema('id'),
                    string_schema('value'),
                )))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_signal_stop(): void
    {
        $generator = from_excel(path_real(__DIR__ . '/../Fixtures/fixture.xlsx'))->extract(flow_context(config()));

        static::assertTrue($generator->valid());
        $generator->next();
        static::assertTrue($generator->valid());
        $generator->next();
        static::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        static::assertFalse($generator->valid());
    }
}
