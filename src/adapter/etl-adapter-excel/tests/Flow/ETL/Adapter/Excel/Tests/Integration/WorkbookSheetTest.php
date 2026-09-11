<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Integration;

use Flow\ETL\Adapter\Excel\ExcelReader;
use Flow\ETL\Adapter\Excel\ExcelReadOptions;
use Flow\ETL\Adapter\Excel\Tests\Context\ExcelFixtureContext;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

use function array_keys;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function iterator_to_array;

final class WorkbookSheetTest extends FlowTestCase
{
    public function test_a_late_abandoned_generator_does_not_drop_a_handle_opened_since(): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());
        $sheet = ExcelFixtureContext::sheet('sniff/a', $filesystem);

        $abandoned = $sheet->rows();
        $abandoned->current();
        $sheet->close();

        $current = $sheet->rows();
        $current->current();
        unset($abandoned);

        // sniff/a is extension-less, so every open costs exactly one readFrom; a third would mean the abandoned
        // generator closed the handle the second rows() opened, forcing columns() to open a fourth time
        static::assertSame(['id', 'name', 'email'], $sheet->columns());
        static::assertSame(2, $filesystem->readFromCalls);
    }

    #[TestWith([1])]
    #[TestWith([4])]
    #[TestWith([10])]
    #[TestWith([11])]
    public function test_rows_after_a_sample_yield_every_row_once(int $sampled): void
    {
        $sheet = ExcelFixtureContext::sheet('fixture.xlsx');
        $taken = 0;

        foreach ($sheet->sample() as $_row) {
            if (++$taken >= $sampled) {
                break;
            }
        }

        static::assertEquals(
            iterator_to_array(ExcelFixtureContext::sheet('fixture.xlsx')->rows(), false),
            iterator_to_array($sheet->rows(), false),
        );
    }

    public function test_rows_after_a_sample_read_on_without_reopening_the_file(): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());
        $sheet = ExcelFixtureContext::sheet('sniff/a', $filesystem);

        iterator_to_array($sheet->sample(), false);

        static::assertEquals(
            iterator_to_array(ExcelFixtureContext::sheet('sniff/a')->rows(), false),
            iterator_to_array($sheet->rows(), false),
        );
        static::assertSame(1, $filesystem->readFromCalls);
    }

    public function test_a_second_close_is_a_no_op(): void
    {
        $sheet = ExcelFixtureContext::sheet('fixture.xlsx');
        $sheet->columns();

        $sheet->close();
        $sheet->close();

        static::assertSame(['id', 'name', 'email'], $sheet->columns());
        static::assertCount(10, iterator_to_array($sheet->rows(), false));
    }

    public function test_an_empty_sheet_has_no_columns_and_no_rows(): void
    {
        $sheet = ExcelFixtureContext::sheet('empty_sheet.xlsx');

        static::assertSame([], $sheet->columns());
        static::assertCount(0, iterator_to_array($sheet->rows(), false));
    }

    public function test_a_format_the_workbook_is_not_is_reported_as_a_failed_open(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Failed to open file: Could not open');

        ExcelFixtureContext::sheet('fixture.xlsx', options: new ExcelReadOptions(format: ExcelReader::ODS))->columns();
    }

    public function test_columns_are_generated_when_header_is_disabled(): void
    {
        static::assertSame(
            ['e00', 'e01', 'e02'],
            ExcelFixtureContext::sheet('fixture.xlsx', options: new ExcelReadOptions(withHeader: false))->columns(),
        );
    }

    public function test_columns_are_the_header_row(): void
    {
        static::assertSame(['id', 'name', 'email'], ExcelFixtureContext::sheet('fixture.xlsx')->columns());
    }

    public function test_a_header_only_sheet_has_columns_and_no_rows(): void
    {
        $sheet = ExcelFixtureContext::sheet('header_only.xlsx');

        static::assertSame(['id', 'name'], $sheet->columns());
        static::assertCount(0, iterator_to_array($sheet->rows(), false));
    }

    public function test_nothing_is_opened_before_first_use(): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());
        $sheet = ExcelFixtureContext::sheet('sniff/a', $filesystem);
        $beforeFirstUse = $filesystem->readFromCalls;

        $sheet->columns();

        static::assertSame(0, $beforeFirstUse);
        static::assertSame(1, $filesystem->readFromCalls);
    }

    #[TestWith(['fixture.xlsx'])]
    #[TestWith(['fixture.ods'])]
    public function test_rows_close_the_reader_when_abandoned(string $fixture): void
    {
        $sheet = ExcelFixtureContext::sheet($fixture);

        foreach ($sheet->rows() as $_) {
            break;
        }

        // a sheet left open would resume where the break left it, or - for ODS, which cannot be reopened
        // on the same OpenSpout instance - throw
        static::assertCount(10, iterator_to_array($sheet->rows(), false));
    }

    #[TestWith(['fixture.xlsx'])]
    #[TestWith(['fixture.ods'])]
    public function test_rows_close_the_reader_when_fully_read(string $fixture): void
    {
        $sheet = ExcelFixtureContext::sheet($fixture);

        static::assertCount(10, iterator_to_array($sheet->rows(), false));
        static::assertCount(10, iterator_to_array($sheet->rows(), false));
    }

    public function test_rows_re_emit_the_row_the_header_was_resolved_from(): void
    {
        $rows = iterator_to_array(
            ExcelFixtureContext::sheet('fixture.xlsx', options: new ExcelReadOptions(withHeader: false))->rows(),
            false,
        );

        static::assertCount(11, $rows);
        static::assertSame(['e00' => 'id', 'e01' => 'name', 'e02' => 'email'], $rows[0]->values);
    }

    public function test_rows_resolve_the_columns_without_a_columns_call_first(): void
    {
        $rows = iterator_to_array(ExcelFixtureContext::sheet('fixture.xlsx')->rows(), false);

        static::assertCount(10, $rows);
        static::assertSame(['id', 'name', 'email'], array_keys($rows[0]->values));
    }

    public function test_an_unknown_sheet_name_names_the_sheet_it_looked_for(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Sheet with name: 'unknown' not found.");

        ExcelFixtureContext::sheet('fixture.xlsx', options: new ExcelReadOptions(sheetName: 'unknown'))->columns();
    }
}
