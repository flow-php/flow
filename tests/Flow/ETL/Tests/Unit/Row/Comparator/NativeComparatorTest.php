<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Comparator;

use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class NativeComparatorTest extends TestCase
{
    public function test_row_comparison() : void
    {
        $row = Row::create(new Row\Entry\StringEntry('test', 'test'));
        $nextRow = Row::create(new Row\Entry\StringEntry('test', 'test'));

        $comparator = new Row\Comparator\NativeComparator();

        $this->assertTrue($comparator->equals($row, $nextRow));
    }

    public function test_row_comparison_for_different_rows() : void
    {
        $row = Row::create(new Row\Entry\StringEntry('test', 'test'));
        $nextRow = Row::create(new Row\Entry\IntegerEntry('test', 2));

        $comparator = new Row\Comparator\NativeComparator();

        $this->assertFalse($comparator->equals($row, $nextRow));
    }

    public function test_compare_rows_with_similar_but_not_the_same_objects() : void
    {
        $row = Row::create(new Row\Entry\ObjectEntry('object', new \DateTimeImmutable('2020-01-01 00:00:00 UTC')));
        $nextRow = Row::create(new Row\Entry\ObjectEntry('object', new \DateTimeImmutable('2020-01-01 00:00:00 UTC')));

        $comparator = new Row\Comparator\NativeComparator();

        $this->assertTrue($comparator->equals($row, $nextRow));
    }
}
