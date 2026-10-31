<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Unit\IO\Inference;

use Flow\ETL\Adapter\Excel\IO\Inference\WorkbookSheetSample;
use Flow\ETL\Adapter\Excel\Tests\Context\CraftedWorkbook;
use Flow\ETL\Adapter\Excel\Tests\Context\ExcelFixtureContext;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\Filesystem\DSL\path_real;
use function iterator_to_array;

final class WorkbookSheetSampleTest extends FlowTestCase
{
    public function test_a_sample_stopped_early_is_not_whole(): void
    {
        $sheet = ExcelFixtureContext::sheet('fixture.xlsx');
        $sample = new WorkbookSheetSample($sheet, $sheet->sample());

        foreach ($sample as $_rowValues) {
            break;
        }

        static::assertFalse($sample->wholeSheet());
        $sheet->close();
    }

    public function test_a_sample_read_to_the_end_is_whole(): void
    {
        $sheet = ExcelFixtureContext::sheet('fixture.xlsx');
        $sample = new WorkbookSheetSample($sheet, $sheet->sample());

        static::assertCount(10, iterator_to_array($sample, false));
        static::assertTrue($sample->wholeSheet());
        $sheet->close();
    }

    public function test_a_sample_never_iterated_is_not_whole(): void
    {
        $sheet = ExcelFixtureContext::sheet('fixture.xlsx');

        static::assertFalse((new WorkbookSheetSample($sheet, $sheet->sample()))->wholeSheet());
        $sheet->close();
    }

    public function test_a_corrupted_record_is_left_out_of_the_sample(): void
    {
        $sheet = ExcelFixtureContext::reader()->sheet(new SourceFile(path_real(CraftedWorkbook::with(CraftedWorkbook::sheet(
            '<row r="1"><c r="A1" t="inlineStr"><is><t>a</t></is></c></row>'
            . '<row r="2"><c r="A2"><v>1</v></c><c r="B2"><v>2</v></c></row>'
            . '<row r="3"><c r="A3"><v>3</v></c></row>',
        )))));
        $sample = new WorkbookSheetSample($sheet, $sheet->sample());

        static::assertSame([['a' => 3]], iterator_to_array($sample, false));
        $sheet->close();
    }
}
