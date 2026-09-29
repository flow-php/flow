<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\HashJoin;

use Flow\ETL\Join\HashJoin\HashTable;
use Flow\ETL\Tests\FlowTestCase;

final class HashTableTest extends FlowTestCase
{
    public function test_candidates_for_matching_hash(): void
    {
        $hashTable = new HashTable();
        $hashTable->add('hash-a', 0);
        $hashTable->add('hash-b', 1);
        $hashTable->add('hash-a', 2);

        static::assertSame([0, 2], $hashTable->candidatesFor('hash-a'));
        static::assertSame([1], $hashTable->candidatesFor('hash-b'));
    }

    public function test_constant_hash_makes_every_row_a_candidate(): void
    {
        $hashTable = new HashTable();
        $hashTable->add('0', 0);
        $hashTable->add('0', 1);

        static::assertSame([0, 1], $hashTable->candidatesFor('0'));
    }

    public function test_count_is_every_added_row(): void
    {
        $hashTable = new HashTable();
        $hashTable->add('hash-a', 0);
        $hashTable->add('hash-a', 1);

        static::assertSame(2, $hashTable->count());
    }

    public function test_no_candidates_for_unknown_hash(): void
    {
        $hashTable = new HashTable();
        $hashTable->add('hash-a', 0);

        static::assertSame([], $hashTable->candidatesFor('hash-404'));
    }

    public function test_unmatched_rows_tracking(): void
    {
        $hashTable = new HashTable(trackUnmatched: true);
        $hashTable->add('hash-a', 0);
        $hashTable->add('hash-b', 1);
        $hashTable->add('hash-c', 2);

        $hashTable->matched(1);

        static::assertSame([0, 2], $hashTable->unmatched());
    }

    public function test_unmatched_rows_without_tracking_are_empty(): void
    {
        $hashTable = new HashTable();
        $hashTable->add('hash-a', 0);

        static::assertSame([], $hashTable->unmatched());
    }
}
