<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Integration\Sheet;

use Flow\ETL\Adapter\Excel\Sheet\XlsxSheetRows;
use Flow\ETL\Adapter\Excel\Tests\Context\CraftedWorkbook;
use Flow\ETL\Adapter\Excel\Tests\Context\ExcelFixtureContext;
use Flow\ETL\Adapter\Excel\Tests\Context\OpenSpoutRows;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use OpenSpout\Reader\Exception\XMLProcessingException;
use OpenSpout\Reader\XLSX\Reader;
use PHPUnit\Framework\Attributes\DataProvider;
use Throwable;

use function iterator_to_array;
use function serialize;
use function str_repeat;

final class XlsxSheetRowsTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{string}>
     */
    public static function crafted(): Generator
    {
        yield 'a dimension pads every row' => [CraftedWorkbook::sheet(
            '<row r="1"><c r="A1"><v>1</v></c><c r="B1" t="inlineStr"><is><t>a</t></is></c></row>'
            . '<row r="2"><c r="C2"><v>2.5</v></c></row>',
            'A1:D2',
        )];
        yield 'no dimension fills the gaps below the highest column' => [CraftedWorkbook::sheet(
            '<row r="1"><c r="A1"><v>1</v></c><c r="D1"><v>4</v></c></row>'
            . '<row r="2"><c r="B2"><v>2</v></c><c r="C2"><v>3</v></c><c r="F2"><v>6</v></c></row>',
        )];
        yield 'spans pad a row' => [CraftedWorkbook::sheet(
            '<row r="1" spans="1:5"><c r="B1"><v>1</v></c></row>',
            'A1:B1',
        )];
        yield 'empty and self-closing rows are skipped' =>
            [CraftedWorkbook::sheet('<row r="1"><c r="A1" t="inlineStr"><is><t></t></is></c></row><row r="2"/>'
            . '<row r="3"><c r="A3"><v>3</v></c></row>')];
        yield 'a cell without a reference follows the previous one' => [CraftedWorkbook::sheet(
            '<row><c><v>1</v></c><c><v>2</v></c><c r="E1"><v>5</v></c><c><v>6</v></c></row>',
        )];
        yield 'booleans, errors, formulas and str cells' => [CraftedWorkbook::sheet(
            '<row r="1"><c r="A1" t="b"><v>1</v></c><c r="B1" t="e"><v>#DIV/0!</v></c>'
            . '<c r="C1"><f>1+1</f><v>2</v></c><c r="D1" t="str"><v> x </v></c><c r="E1" t="n"></c></row>',
        )];
        yield 'escaped control characters' => [CraftedWorkbook::sheet(
            '<row r="1"><c r="A1" t="inlineStr"><is><t>a_x0009_b</t><t>_x005F_x0041_</t></is></c></row>',
        )];
        yield 'a three-letter column' => [CraftedWorkbook::sheet('<row r="1"><c r="AAA1"><v>1</v></c></row>')];
        yield 'prefixed element names' => [
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                . '<x:worksheet xmlns:x="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                . '<x:dimension ref="A1:B1"/><x:sheetData><x:row r="1"><x:c r="A1"><x:v>1</x:v></x:c>'
                . '<x:c r="B1" t="inlineStr"><x:is><x:t>b</x:t></x:is></x:c></x:row></x:sheetData></x:worksheet>',
        ];
    }

    /**
     * @return Generator<string, array{string}>
     */
    public static function refused(): Generator
    {
        yield 'an invalid cell reference' => [CraftedWorkbook::sheet('<row r="1"><c r="1A"><v>1</v></c></row>')];
        yield 'a four-letter column' => [CraftedWorkbook::sheet('<row r="1"><c r="AAAA1"><v>1</v></c></row>')];
    }

    /**
     * @return Generator<string, array{string}>
     */
    public static function workbooks(): Generator
    {
        foreach ([
            'cross_stream/date=2026-01-01/data.xlsx',
            'dates_mixed.xlsx',
            'dates_only.xlsx',
            'diverging_header/a.xlsx',
            'empty_first/a.xlsx',
            'empty_sheet.xlsx',
            'fixture.xlsx',
            'header_only.xlsx',
            'int_float_bool.xlsx',
            'int_then_str.xlsx',
            'mixed_formats/a.xlsx',
            'nullable_fixture.xlsx',
            'orders_1k.xlsx',
            'orders_flow.xlsx',
            'partitioned/group=1/file_01.xlsx',
        ] as $fixture) {
            yield $fixture => [ExcelFixtureContext::file($fixture)];
        }
    }

    #[DataProvider('crafted')]
    public function test_a_crafted_sheet_reads_as_openspout_reads_it(string $sheetXml): void
    {
        $reader = new Reader();
        $reader->open(CraftedWorkbook::with($sheetXml));

        try {
            foreach ($reader->getSheetIterator() as $sheet) {
                static::assertSame(
                    serialize(OpenSpoutRows::of($sheet)),
                    serialize(iterator_to_array((new XlsxSheetRows($sheet))->values(), false)),
                );

                break;
            }
        } finally {
            $reader->close();
        }
    }

    /**
     * OpenSpout itself is no oracle here: its RowIterator asserts the expanded node, so the same sheet ends in an
     * AssertionError, or a TypeError once assertions are off.
     */
    public function test_a_malformed_sheet_is_refused_with_the_libxml_error(): void
    {
        $reader = new Reader();
        // OpenSpout's header reader parses the first buffer while the workbook opens, so the fault sits past it
        $reader->open(CraftedWorkbook::with(CraftedWorkbook::sheet(
            str_repeat('<row><c t="inlineStr"><is><t>padding past the first libxml buffer</t></is></c></row>', 2000)
                . '<row><c r="A2001"><v>1</v></row>',
        )));

        try {
            foreach ($reader->getSheetIterator() as $sheet) {
                $this->expectException(XMLProcessingException::class);
                $this->expectExceptionMessage('Opening and ending tag mismatch: c line 1 and row');

                iterator_to_array((new XlsxSheetRows($sheet))->values(), false);
            }
        } finally {
            $reader->close();
        }
    }

    #[DataProvider('refused')]
    public function test_a_sheet_openspout_refuses_is_refused_with_its_exception(string $sheetXml): void
    {
        $reader = new Reader();
        $reader->open(CraftedWorkbook::with($sheetXml));

        try {
            foreach ($reader->getSheetIterator() as $sheet) {
                $expected = null;

                try {
                    OpenSpoutRows::of($sheet);
                } catch (Throwable $refusal) {
                    $expected = $refusal::class;
                }

                static::assertNotNull($expected);

                $this->expectException($expected);

                iterator_to_array((new XlsxSheetRows($sheet))->values(), false);
            }
        } finally {
            $reader->close();
        }
    }

    /**
     * OpenSpout reads a spans attribute without a colon through an undefined array key (a PHP warning) as width 0.
     */
    public function test_spans_without_a_colon_pad_nothing(): void
    {
        $reader = new Reader();
        $reader->open(CraftedWorkbook::with(CraftedWorkbook::sheet(
            '<row r="1" spans="3"><c r="B1"><v>1</v></c></row>',
        )));

        try {
            foreach ($reader->getSheetIterator() as $sheet) {
                static::assertSame([['', 1]], iterator_to_array((new XlsxSheetRows($sheet))->values(), false));

                break;
            }
        } finally {
            $reader->close();
        }
    }

    #[DataProvider('workbooks')]
    public function test_every_sheet_reads_as_openspout_reads_it(string $file): void
    {
        $reader = new Reader();
        $reader->open($file);

        try {
            foreach ($reader->getSheetIterator() as $sheet) {
                static::assertSame(
                    serialize(OpenSpoutRows::of($sheet)),
                    serialize(iterator_to_array((new XlsxSheetRows($sheet))->values(), false)),
                );
            }
        } finally {
            $reader->close();
        }
    }
}
