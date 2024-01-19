<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Comparator;

use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class WeakObjectComparatorTest extends TestCase
{
    public function test_compare_rows_different_entries() : void
    {
        $row = Row::create(
            new Row\Entry\ObjectEntry('object', new \ArrayObject([])),
            new Row\Entry\StringEntry('string', 'test'),
        );
        $nextRow = Row::create(
            new Row\Entry\StringEntry('string', 'test1'),
        );

        $comparator = new Row\Comparator\WeakObjectComparator();

        $this->assertFalse($comparator->equals($row, $nextRow));
    }

    public function test_compare_rows_with_other_different_entries() : void
    {
        $row = Row::create(
            new Row\Entry\ObjectEntry('object', new \ArrayObject([])),
            new Row\Entry\StringEntry('string', 'test'),
        );
        $nextRow = Row::create(
            new Row\Entry\ObjectEntry('object', new \ArrayObject([])),
            new Row\Entry\StringEntry('string', 'test1'),
        );

        $comparator = new Row\Comparator\WeakObjectComparator();

        $this->assertFalse($comparator->equals($row, $nextRow));
    }
}
