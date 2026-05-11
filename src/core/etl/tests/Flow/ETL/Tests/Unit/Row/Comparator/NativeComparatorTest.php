<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Comparator;

use Flow\ETL\Row\Comparator\NativeComparator;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\integer_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\string_entry;

final class NativeComparatorTest extends FlowTestCase
{
    public function test_row_comparison(): void
    {
        $row = row(string_entry('test', 'test'));
        $nextRow = row(string_entry('test', 'test'));

        $comparator = new NativeComparator();

        static::assertTrue($comparator->equals($row, $nextRow));
    }

    public function test_row_comparison_for_different_rows(): void
    {
        $row = row(string_entry('test', 'test'));
        $nextRow = row(integer_entry('test', 2));

        $comparator = new NativeComparator();

        static::assertFalse($comparator->equals($row, $nextRow));
    }
}
