<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Kernels\HashJoin;

use Flow\ETL\Kernels\HashJoin\HashTable;
use Flow\ETL\Tests\FlowTestCase;

final class HashTableTest extends FlowTestCase
{
    public function test_candidates_of_a_group(): void
    {
        $hashTable = new HashTable();
        $hashTable->add(10, 0);
        $hashTable->add(11, 1);
        $hashTable->add(10, 2);

        static::assertSame([0, 2], $hashTable->candidatesFor(10));
        static::assertSame([1], $hashTable->candidatesFor(11));
    }

    public function test_one_group_makes_every_row_a_candidate(): void
    {
        $hashTable = new HashTable();
        $hashTable->add(0, 0);
        $hashTable->add(0, 1);

        static::assertSame([0, 1], $hashTable->candidatesFor(0));
    }

    public function test_count_is_every_added_row(): void
    {
        $hashTable = new HashTable();
        $hashTable->add(10, 0);
        $hashTable->add(10, 1);

        static::assertSame(2, $hashTable->count());
    }

    public function test_no_candidates_for_an_unknown_group(): void
    {
        $hashTable = new HashTable();
        $hashTable->add(10, 0);

        static::assertSame([], $hashTable->candidatesFor(404));
    }

    public function test_unmatched_rows_tracking(): void
    {
        $hashTable = new HashTable(trackUnmatched: true);
        $hashTable->add(10, 0);
        $hashTable->add(11, 1);
        $hashTable->add(12, 2);

        $hashTable->matched(1);

        static::assertSame([0, 2], $hashTable->unmatched());
    }

    public function test_unmatched_rows_without_tracking_are_empty(): void
    {
        $hashTable = new HashTable();
        $hashTable->add(10, 0);

        static::assertSame([], $hashTable->unmatched());
    }
}
