<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Integration;

use function Flow\ETL\Adapter\Excel\DSL\from_excel;
use function Flow\ETL\DSL\{config, df, flow_context};
use Flow\ETL\Adapter\Excel\ExcelReader;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{Extractor\Signal, Row, Rows};
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

        $total = 0;

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            $rows->each(function (Row $row) : void {
                $this->assertSame(
                    ['id', 'name', 'email'],
                    \array_keys($row->toArray())
                );
            });
            $total += $rows->count();
        }

        self::assertSame(5, $total);
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_offset(string $fixtureName) : void
    {
        $extractor = from_excel($fixtureName);
        $extractor->withOffset(5);

        $total = 0;

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            $rows->each(function (Row $row) : void {
                $this->assertSame(
                    ['id', 'name', 'email'],
                    \array_keys($row->toArray())
                );
            });
            $total += $rows->count();
        }

        self::assertSame(7, $total);
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_offset_without_header(string $fixtureName) : void
    {
        $extractor = from_excel($fixtureName);
        $extractor->withHeader(false);
        $extractor->withOffset(5);

        $total = 0;

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            $rows->each(function (Row $row) : void {
                $this->assertSame(
                    ['e00', 'e01', 'e02'],
                    \array_keys($row->toArray())
                );
            });
            $total += $rows->count();
        }

        self::assertSame(6, $total);
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_selected_sheet_name(string $fixtureName) : void
    {
        $extractor = from_excel($fixtureName);
        $extractor->withSheetName('Sheet2');

        $total = 0;

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            $rows->each(function (Row $row) : void {
                $this->assertSame(
                    ['id', 'name', 'email'],
                    \array_keys($row->toArray())
                );
            });
            $total += $rows->count();
        }

        self::assertSame(5, $total);
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_unknown_sheet_name(string $fixtureName) : void
    {
        $extractor = from_excel($fixtureName);
        $extractor->withSheetName('unknown');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Sheet with name: 'unknown' not found.");

        iterator_to_array($extractor->extract(flow_context(config())));
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_without_header(string $fixtureName) : void
    {
        $extractor = from_excel($fixtureName);
        $extractor->withHeader(false);

        $total = 0;

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            $rows->each(function (Row $row) : void {
                $this->assertSame(
                    ['e00', 'e01', 'e02'],
                    \array_keys($row->toArray())
                );
            });
            $total += $rows->count();
        }

        self::assertSame(10, $total);
    }

    #[DataProvider('provide_nullable_fixtures')]
    public function test_extract_excel_nullable_file(string $fixtureName) : void
    {
        $extractor = from_excel($fixtureName);

        $total = 0;

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            $rows->each(function (Row $row) : void {
                $this->assertSame(['id', 'name', 'email'], \array_keys($row->toArray()));
                $this->assertCount(3, $row->toArray());
            });
            $total += $rows->count();
        }

        self::assertSame(5, $total);
    }

    public function test_extract_with_wrongly_selected_reader() : void
    {
        $extractor = from_excel(__DIR__ . '/../Fixtures/fixture.xlsx');
        $extractor->withReader(ExcelReader::ODS);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Failed to open file: Could not open');

        iterator_to_array($extractor->extract(flow_context(config())));
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
