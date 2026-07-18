<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\HashJoin;

use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Join\HashJoin\EqualityJoinKeys;
use Flow\ETL\Join\HashJoin\HashTable;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;
use function iterator_to_array;

final class HashTableTest extends FlowTestCase
{
    public function test_candidates_for_matching_left_row(): void
    {
        $keys = EqualityJoinKeys::fromComparison(new Equal('id', 'user_id'));
        static::assertNotNull($keys);

        $hashTable = new HashTable($keys);
        $hashTable->add(row(int_entry('user_id', 1), str_entry('name', 'Alice')));
        $hashTable->add(row(int_entry('user_id', 2), str_entry('name', 'Bob')));

        $candidates = $hashTable->candidatesFor(row(int_entry('id', 1)));

        static::assertCount(1, $candidates);
        static::assertSame('Alice', $candidates[0]->valueOf('name'));
    }

    public function test_duplicated_rows_are_preserved(): void
    {
        $keys = EqualityJoinKeys::fromComparison(new Equal('id', 'user_id'));
        static::assertNotNull($keys);

        $hashTable = new HashTable($keys);
        $hashTable->add(row(int_entry('user_id', 1), str_entry('name', 'Alice')));
        $hashTable->add(row(int_entry('user_id', 1), str_entry('name', 'Alice')));

        static::assertSame(2, $hashTable->count());
        static::assertCount(2, $hashTable->candidatesFor(row(int_entry('id', 1))));
    }

    public function test_no_candidates_for_unknown_key(): void
    {
        $keys = EqualityJoinKeys::fromComparison(new Equal('id', 'user_id'));
        static::assertNotNull($keys);

        $hashTable = new HashTable($keys);
        $hashTable->add(row(int_entry('user_id', 1)));

        static::assertSame([], $hashTable->candidatesFor(row(int_entry('id', 404))));
    }

    public function test_unmatched_rows_tracking(): void
    {
        $keys = EqualityJoinKeys::fromComparison(new Equal('id', 'user_id'));
        static::assertNotNull($keys);

        $hashTable = new HashTable($keys, trackUnmatched: true);
        $hashTable->add(row(int_entry('user_id', 1), str_entry('name', 'Alice')));
        $hashTable->add(row(int_entry('user_id', 2), str_entry('name', 'Bob')));
        $hashTable->add(row(int_entry('user_id', 3), str_entry('name', 'Cid')));

        $hashTable->matched(1);

        $unmatched = iterator_to_array($hashTable->unmatchedRows(), false);

        static::assertCount(2, $unmatched);
        static::assertSame('Alice', $unmatched[0]->valueOf('name'));
        static::assertSame('Cid', $unmatched[1]->valueOf('name'));
    }

    public function test_unmatched_rows_without_tracking_are_empty(): void
    {
        $keys = EqualityJoinKeys::fromComparison(new Equal('id', 'user_id'));
        static::assertNotNull($keys);

        $hashTable = new HashTable($keys);
        $hashTable->add(row(int_entry('user_id', 1)));

        static::assertSame([], iterator_to_array($hashTable->unmatchedRows(), false));
    }
}
