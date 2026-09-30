<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Writer\ColumnLists;
use PHPUnit\Framework\TestCase;

final class ColumnListsTest extends TestCase
{
    public function test_the_length_every_list_has(): void
    {
        static::assertSame(2, (new ColumnLists())->length(['id' => [1, 2], 'name' => ['a', null]]));
        static::assertSame(0, (new ColumnLists())->length(['id' => [], 'name' => []]));
    }

    public function test_no_lists_hold_no_rows(): void
    {
        static::assertSame(0, (new ColumnLists())->length([]));
    }

    public function test_lists_of_different_lengths_are_refused_with_every_length(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('writeColumns() takes lists of one length, got "id": 3, "name": 2, "tags": 3');

        (new ColumnLists())->length(['id' => [1, 2, 3], 'name' => ['a', 'b'], 'tags' => [[], [], []]]);
    }
}
