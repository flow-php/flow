<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit;

use Flow\ETL\Adapter\GoogleSheet\SheetAddress;
use PHPUnit\Framework\TestCase;

final class SheetAddressTest extends TestCase
{
    public function test_calculating() : void
    {
        $range = SheetAddress::calculate(5, 10);

        self::assertEquals('A1', $range->getStartCell());
        self::assertEquals('E10', $range->getEndCell());
    }

    public function test_calculating_the_from_a_range() : void
    {
        $range = SheetAddress::calculate(2, 10, new SheetAddress('A1', 'B5', 'Sheet1'));

        self::assertEquals('A1', $range->getStartCell());
        self::assertEquals('B10', $range->getEndCell());
        self::assertEquals('A', $range->getStartCellColumn());
        self::assertEquals(1, $range->getStartCellRow());
        self::assertEquals('B', $range->getEndCellColumn());
        self::assertEquals(10, $range->getEndCellRow());
        self::assertEquals('Sheet1', $range->getSheetName());
    }

    public function test_with_multi_words_sheet_name() : void
    {
        $range = SheetAddress::calculate(2, 10, new SheetAddress('A1', 'B5', 'Sheet 1'));

        self::assertEquals('Sheet 1', $range->getSheetName());
        self::assertEquals("'Sheet 1'!A1:B10", $range->toString());
    }
}
