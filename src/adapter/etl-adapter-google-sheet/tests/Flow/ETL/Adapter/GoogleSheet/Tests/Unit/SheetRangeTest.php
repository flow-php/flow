<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit;

use Flow\ETL\Adapter\GoogleSheet\Columns;
use Flow\ETL\Adapter\GoogleSheet\SheetRange;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

final class SheetRangeTest extends FlowTestCase
{
    public static function example_string_ranges(): Generator
    {
        yield 'one cell' => [
            new SheetRange(new Columns('Sheet2', 'B', 'B'), 2, 2, 10),
            'Sheet2!B2:B2',
        ];
        yield 'one line range' => [
            new SheetRange(new Columns('Sheet1', 'A', 'C'), 1, 1, 10),
            'Sheet1!A1:C1',
        ];
        yield 'multiple line range' => [
            new SheetRange(new Columns('Sheet1', 'B', 'D'), 2, 30, 100),
            'Sheet1!B2:D30',
        ];
        yield 'multi letter columns' => [
            new SheetRange(new Columns('Sheet1', 'ABC', 'CBA'), 101, 999, 1000),
            'Sheet1!ABC101:CBA999',
        ];
        yield 'end row greater than max rows' => [
            new SheetRange(new Columns('Sheet1', 'ABC', 'CBA'), 101, 999, 950),
            'Sheet1!ABC101:CBA950',
        ];
    }

    public static function invalid_cases(): Generator
    {
        yield 'start row under 0' => [
            0,
            1,
            100,
            'Start row "0" must be greater than 0',
        ];
        yield 'end row under 0' => [
            1,
            0,
            100,
            'End row "0" must be greater than 0',
        ];
        yield 'end row greater or equal to start row 0' => [
            19,
            10,
            100,
            'End row "10" must be greater or equal to start row "19"',
        ];
        yield 'max row under 1' => [
            1,
            1,
            0,
            'Max rows "0" must be greater than 0',
        ];
    }

    #[DataProvider('invalid_cases')]
    public function test_assertions(int $startRow, int $endRow, int $maxRows, string $expectedExceptionMessage): void
    {
        $this->expectExceptionMessage($expectedExceptionMessage);
        $this->expectException(InvalidArgumentException::class);

        new SheetRange(new Columns('Sheet2', 'A', 'B'), $startRow, $endRow, $maxRows);
    }

    public function test_next_rows_range(): void
    {
        $range = new SheetRange(new Columns('Sheet2', 'A', 'B'), 1, 10, 100);
        static::assertSame('Sheet2!A11:B20', $range->nextRows(10)->toString());
        static::assertSame('Sheet2!A21:B40', $range->nextRows(10)->nextRows(20)->toString());
    }

    public function test_next_rows_range_with_amount_greater_than_max_rows(): void
    {
        $range = new SheetRange(new Columns('Sheet2', 'A', 'B'), 1, 10, 99);
        static::assertSame('Sheet2!A11:B60', $range->nextRows(50)->toString());
        static::assertSame('Sheet2!A11:B99', $range->nextRows(100)->toString());
    }

    #[DataProvider('example_string_ranges')]
    public function test_range_to_string(SheetRange $range, string $expectedStringRange): void
    {
        static::assertSame($expectedStringRange, $range->toString());
    }
}
