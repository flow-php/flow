<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Integration;

use function Flow\ETL\Adapter\Excel\DSL\from_excel;
use function Flow\ETL\DSL\{config, df, flow_context, int_schema, schema, string_schema};
use Flow\ETL\Adapter\Excel\ExcelReader;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{Extractor\Signal, Rows};
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\{Partition, Path};
use PHPUnit\Framework\Attributes\DataProvider;

final class ExcelExtractorTest extends FlowTestCase
{
    /**
     * @return iterable<string, array<string>>
     */
    public static function provide_fixtures() : iterable
    {
        yield 'ods' => [__DIR__ . '/../Fixtures/fixture.ods'];
        yield 'xlsx' => [__DIR__ . '/../Fixtures/fixture.xlsx'];
        yield 'alike ods' => [__DIR__ . '/../Fixtures/fixture_as_ods'];
        yield 'alike xlsx' => [__DIR__ . '/../Fixtures/fixture_as_xlsx'];
    }

    /**
     * @return iterable<string, array<string>>
     */
    public static function provide_nullable_fixtures() : iterable
    {
        yield 'ods' => [__DIR__ . '/../Fixtures/nullable_fixture.ods'];
        yield 'xlsx' => [__DIR__ . '/../Fixtures/nullable_fixture.xlsx'];
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file(string $fixtureName) : void
    {
        $rows = df()
            ->extract(from_excel($fixtureName))
            ->fetch()
            ->toArray();

        self::assertCount(10, $rows);
        self::assertNull($rows[9]['email']);
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_empty_cells(string $fixtureName) : void
    {
        $rows = df()
            ->extract(
                from_excel($fixtureName)
                    ->withConvertEmptyToNull(false)
            )
            ->fetch()
            ->toArray();

        self::assertCount(10, $rows);
        self::assertEmpty($rows[9]['email']);
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_limit(string $fixtureName) : void
    {
        $extractor = from_excel($fixtureName);
        $extractor->changeLimit(5);

        $rows = df()
            ->extract($extractor)
            ->fetch()
            ->toArray();

        self::assertCount(5, $rows);

        foreach ($rows as $row) {
            self::assertSame(['id', 'name', 'email'], \array_keys($row));
            self::assertCount(3, $row);
        }
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_offset(string $fixtureName) : void
    {
        $extractor = from_excel($fixtureName);
        $extractor->withOffset(5);

        $rows = df()
            ->extract($extractor)
            ->fetch()
            ->toArray();

        self::assertCount(7, $rows);

        foreach ($rows as $row) {
            self::assertSame(['id', 'name', 'email'], \array_keys($row));
            self::assertCount(3, $row);
        }
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_offset_without_header(string $fixtureName) : void
    {
        $extractor = from_excel($fixtureName);
        $extractor->withHeader(false);
        $extractor->withOffset(5);

        $rows = df()
            ->extract($extractor)
            ->fetch()
            ->toArray();

        self::assertCount(6, $rows);

        foreach ($rows as $row) {
            self::assertSame(['e00', 'e01', 'e02'], \array_keys($row));
            self::assertCount(3, $row);
        }
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_selected_sheet_name(string $fixtureName) : void
    {
        $rows = df()
            ->extract(
                from_excel($fixtureName)
                    ->withSheetName('Sheet2')
            )
            ->fetch()
            ->toArray();

        self::assertCount(5, $rows);

        foreach ($rows as $row) {
            self::assertSame(['id', 'name', 'email'], \array_keys($row));
            self::assertCount(3, $row);
        }
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_unknown_sheet_name(string $fixtureName) : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Sheet with name: 'unknown' not found.");

        df()
            ->extract(
                from_excel($fixtureName)
                    ->withSheetName('unknown')
            )
            ->fetch()
            ->toArray();
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_without_header(string $fixtureName) : void
    {
        $rows = df()
            ->extract(
                from_excel($fixtureName)
                    ->withHeader(false)
            )
            ->fetch()
            ->toArray();

        self::assertCount(10, $rows);

        foreach ($rows as $row) {
            self::assertSame(['e00', 'e01', 'e02'], \array_keys($row));
            self::assertCount(3, $row);
        }
    }

    #[DataProvider('provide_nullable_fixtures')]
    public function test_extract_excel_nullable_file(string $fixtureName) : void
    {
        $rows = df()
            ->extract(from_excel($fixtureName))
            ->fetch()
            ->toArray();

        self::assertCount(5, $rows);

        foreach ($rows as $row) {
            self::assertSame(['id', 'name', 'email'], \array_keys($row));
            self::assertCount(3, $row);
        }
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_puts_null_in_not_matching_schema_rows(string $fixtureName) : void
    {
        $rows = df()
            ->extract(
                from_excel($fixtureName)
                    ->withSchema(
                        schema(
                            int_schema('id'),
                            string_schema('name'),
                            string_schema('email'),
                            string_schema('missing'),
                        )
                    )
            )
            ->fetch()
            ->toArray();

        foreach ($rows as $row) {
            self::assertNotSame([], $row);
            self::assertNull($row['missing']);
        }
    }

    public function test_extract_with_unknown_file() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unsupported file format: n/a');

        df()
            ->extract(from_excel(__DIR__ . '/../Fixtures/empty_file'))
            ->fetch()
            ->toArray();
    }

    public function test_extract_with_wrongly_selected_reader() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Failed to open file: Could not open');

        df()
            ->extract(
                from_excel(__DIR__ . '/../Fixtures/fixture.xlsx')
                    ->withReader(ExcelReader::ODS)
            )
            ->fetch()
            ->toArray();
    }

    public function test_loading_data_from_all_partitions() : void
    {
        df()
            ->read(from_excel(__DIR__ . '/../Fixtures/partitioned/group=*/*.xlsx'))
            ->run(function (Rows $rows) : void {
                $this->assertSame(
                    ['group'],
                    \array_map(
                        fn (Partition $p) => $p->name,
                        $rows->partitions()->toArray()
                    )
                );
            });
    }

    public function test_signal_stop() : void
    {
        $generator = from_excel(Path::realpath(__DIR__ . '/../Fixtures/fixture.xlsx'))
            ->extract(flow_context(config()));

        self::assertTrue($generator->valid());
        $generator->next();
        self::assertTrue($generator->valid());
        $generator->next();
        self::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        self::assertFalse($generator->valid());
    }
}
