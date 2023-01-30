<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit;

use Flow\ETL\Adapter\GoogleSheet\GoogleSheetCellsRange;
use Flow\ETL\Adapter\GoogleSheet\GoogleSheetColumnRange;
use PHPUnit\Framework\TestCase;

final class GoogleSheetCellsRangeTest extends TestCase
{
    public function example_string_ranges() : \Generator
    {
        yield 'one cell' => [
            new GoogleSheetCellsRange(new GoogleSheetColumnRange('Sheet2', 'B', 'B'), 2, 2),
            'Sheet2!B2:B2',
        ];
        yield 'one line range' => [
            new GoogleSheetCellsRange(new GoogleSheetColumnRange('Sheet1', 'A', 'C'), 1, 1),
            'Sheet1!A1:C1',
        ];
        yield 'multiple line range' => [
            new GoogleSheetCellsRange(new GoogleSheetColumnRange('Sheet1', 'B', 'D'), 2, 30),
            'Sheet1!B2:D30',
        ];
        yield 'multi letter columns' => [
            new GoogleSheetCellsRange(new GoogleSheetColumnRange('Sheet1', 'ABC', 'CBA'), 101, 999),
            'Sheet1!ABC101:CBA999',
        ];
    }

    public function test_next_rows_range() : void
    {
        $range = new GoogleSheetCellsRange(new GoogleSheetColumnRange('Sheet2', 'A', 'B'), 1, 10);
        $this->assertSame('Sheet2!A11:B20', $range->nextRows(10)->toString());
        $this->assertSame('Sheet2!A21:B40', $range->nextRows(10)->nextRows(20)->toString());
    }

    /**
     * @dataProvider example_string_ranges
     */
    public function test_range_to_string(GoogleSheetCellsRange $range, string $expectedStringRange) : void
    {
        $this->assertSame($expectedStringRange, $range->toString());
    }
}
