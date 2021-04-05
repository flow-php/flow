<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Comparator;

use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class WeakObjectComparatorTest extends TestCase
{
    public function test_compare_rows_with_similar_but_not_the_same_objects() : void
    {
        $row = Row::create(new Row\Entry\ObjectEntry('object', new \DateTimeImmutable('2020-01-01 00:00:00 UTC')));
        $nextRow = Row::create(new Row\Entry\ObjectEntry('object', new \DateTimeImmutable('2020-01-01 00:00:00 UTC')));

        $comparator = new Row\Comparator\WeakObjectComparator();

        $this->assertTrue($comparator->equals($row, $nextRow));
    }

    public function test_compare_rows_with_other_different_entries() : void
    {
        $row = Row::create(
            new Row\Entry\ObjectEntry('object', new \DateTimeImmutable('2020-01-01 00:00:00 UTC')),
            new Row\Entry\StringEntry('string', 'test'),
        );
        $nextRow = Row::create(
            new Row\Entry\ObjectEntry('object', new \DateTimeImmutable('2020-01-01 00:00:00 UTC')),
            new Row\Entry\StringEntry('string', 'test1'),
        );

        $comparator = new Row\Comparator\WeakObjectComparator();

        $this->assertFalse($comparator->equals($row, $nextRow));
    }

    public function test_compare_rows_different_entries() : void
    {
        $row = Row::create(
            new Row\Entry\ObjectEntry('object', new \DateTimeImmutable('2020-01-01 00:00:00 UTC')),
            new Row\Entry\StringEntry('string', 'test'),
        );
        $nextRow = Row::create(
            new Row\Entry\StringEntry('string', 'test1'),
        );

        $comparator = new Row\Comparator\WeakObjectComparator();

        $this->assertFalse($comparator->equals($row, $nextRow));
    }
}
