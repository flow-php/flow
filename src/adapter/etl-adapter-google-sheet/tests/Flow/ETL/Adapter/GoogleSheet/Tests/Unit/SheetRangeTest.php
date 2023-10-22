<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit;

use Flow\ETL\Adapter\GoogleSheet\Columns;
use Flow\ETL\Adapter\GoogleSheet\SheetRange;
use Flow\ETL\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class SheetRangeTest extends TestCase
{
    public static function example_string_ranges() : \Generator
    {
        yield 'one cell' => [
            new SheetRange(new Columns('Sheet2', 'B', 'B'), 2, 2),
            'Sheet2!B2:B2',
        ];
        yield 'one line range' => [
            new SheetRange(new Columns('Sheet1', 'A', 'C'), 1, 1),
            'Sheet1!A1:C1',
        ];
        yield 'multiple line range' => [
            new SheetRange(new Columns('Sheet1', 'B', 'D'), 2, 30),
            'Sheet1!B2:D30',
        ];
        yield 'multi letter columns' => [
            new SheetRange(new Columns('Sheet1', 'ABC', 'CBA'), 101, 999),
            'Sheet1!ABC101:CBA999',
        ];
    }

    public static function invalid_cases() : \Generator
    {
        yield 'start row under 0' => [
            0, 1,
            'Start row "0" must be greater than 0',
        ];
        yield 'end row under 0' => [
            1, 0,
            'End row "0" must be greater than 0',
        ];
        yield 'end row greater or equal to start row 0' => [
            19, 10,
            'End row "10" must be greater or equal to start row "19"',
        ];
    }

    /**
     * @dataProvider invalid_cases
     */
    public function test_assertions(int $startRow, int $endRow, string $expectedExceptionMessage) : void
    {
        $this->expectExceptionMessage($expectedExceptionMessage);
        $this->expectException(InvalidArgumentException::class);

        new SheetRange(new Columns('Sheet2', 'A', 'B'), $startRow, $endRow);
    }

    public function test_next_rows_range() : void
    {
        $range = new SheetRange(new Columns('Sheet2', 'A', 'B'), 1, 10);
        $this->assertSame('Sheet2!A11:B20', $range->nextRows(10)->toString());
        $this->assertSame('Sheet2!A21:B40', $range->nextRows(10)->nextRows(20)->toString());
    }

    /**
     * @dataProvider example_string_ranges
     */
    public function test_range_to_string(SheetRange $range, string $expectedStringRange) : void
    {
        $this->assertSame($expectedStringRange, $range->toString());
    }
}
