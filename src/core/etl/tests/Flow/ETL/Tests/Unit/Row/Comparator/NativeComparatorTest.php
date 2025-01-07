<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Comparator;

use function Flow\ETL\DSL\{integer_entry, row, string_entry};
use Flow\ETL\Row\Comparator\NativeComparator;
use Flow\ETL\Tests\FlowTestCase;

final class NativeComparatorTest extends FlowTestCase
{
    public function test_row_comparison() : void
    {
        $row = row(string_entry('test', 'test'));
        $nextRow = row(string_entry('test', 'test'));

        $comparator = new NativeComparator();

        self::assertTrue($comparator->equals($row, $nextRow));
    }

    public function test_row_comparison_for_different_rows() : void
    {
        $row = row(string_entry('test', 'test'));
        $nextRow = row(integer_entry('test', 2));

        $comparator = new NativeComparator();

        self::assertFalse($comparator->equals($row, $nextRow));
    }
}
