<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit;

use Flow\ETL\Adapter\GoogleSheet\GoogleSheetRange;
use PHPUnit\Framework\TestCase;

final class GoogleSheetRangeTest extends TestCase
{
    public function example_string_ranges() : \Generator
    {
        yield 'one cell' => [
            GoogleSheetRange::create('Sheet2', 'B', 2, 'B', 2),
            'Sheet2!B2:B2',
        ];
        yield 'one line range' => [
            GoogleSheetRange::create('Sheet1', 'A', 1, 'C', 1),
            'Sheet1!A1:C1',
        ];
        yield 'multiple line range' => [
            GoogleSheetRange::create('Sheet1', 'B', 2, 'D', 30),
            'Sheet1!B2:D30',
        ];
        yield 'multi letter columns' => [
            GoogleSheetRange::create('Sheet1', 'ABC', 101, 'CBA', 999),
            'Sheet1!ABC101:CBA999',
        ];
    }

    public function test_next_rows_range() : void
    {
        $range = GoogleSheetRange::create('Sheet2', 'A', 1, 'B', 10);
        $this->assertSame('Sheet2!A11:B20', $range->nextRows(10)->toString());
        $this->assertSame('Sheet2!A21:B40', $range->nextRows(10)->nextRows(20)->toString());
    }

    /**
     * @dataProvider example_string_ranges
     */
    public function test_range_to_string(GoogleSheetRange $range, string $expectedStringRange) : void
    {
        $this->assertSame($expectedStringRange, $range->toString());
    }
}
