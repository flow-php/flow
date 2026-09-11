<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\HashJoin;

use Flow\ETL\Join\HashJoin\HashTable;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\row;
use function iterator_to_array;

final class HashTableTest extends FlowTestCase
{
    public function test_candidates_for_matching_hash(): void
    {
        $hashTable = new HashTable();
        $hashTable->add('hash-a', row(['user_id' => 1, 'name' => 'Alice']));
        $hashTable->add('hash-b', row(['user_id' => 2, 'name' => 'Bob']));

        $candidates = $hashTable->candidatesFor('hash-a');

        static::assertCount(1, $candidates);
        static::assertSame('Alice', $candidates[0]->get('name'));
    }

    public function test_constant_hash_makes_every_row_a_candidate(): void
    {
        $hashTable = new HashTable();
        $hashTable->add('0', row(['user_id' => 1]));
        $hashTable->add('0', row(['user_id' => 2]));

        static::assertCount(2, $hashTable->candidatesFor('0'));
    }

    public function test_duplicated_rows_are_preserved(): void
    {
        $hashTable = new HashTable();
        $hashTable->add('hash-a', row(['user_id' => 1, 'name' => 'Alice']));
        $hashTable->add('hash-a', row(['user_id' => 1, 'name' => 'Alice']));

        static::assertSame(2, $hashTable->count());
        static::assertCount(2, $hashTable->candidatesFor('hash-a'));
    }

    public function test_no_candidates_for_unknown_hash(): void
    {
        $hashTable = new HashTable();
        $hashTable->add('hash-a', row(['user_id' => 1]));

        static::assertSame([], $hashTable->candidatesFor('hash-404'));
    }

    public function test_unmatched_rows_tracking(): void
    {
        $hashTable = new HashTable(trackUnmatched: true);
        $hashTable->add('hash-a', row(['user_id' => 1, 'name' => 'Alice']));
        $hashTable->add('hash-b', row(['user_id' => 2, 'name' => 'Bob']));
        $hashTable->add('hash-c', row(['user_id' => 3, 'name' => 'Cid']));

        $hashTable->matched(1);

        $unmatched = iterator_to_array($hashTable->unmatchedRows(), false);

        static::assertCount(2, $unmatched);
        static::assertSame('Alice', $unmatched[0]->get('name'));
        static::assertSame('Cid', $unmatched[1]->get('name'));
    }

    public function test_unmatched_rows_without_tracking_are_empty(): void
    {
        $hashTable = new HashTable();
        $hashTable->add('hash-a', row(['user_id' => 1]));

        static::assertSame([], iterator_to_array($hashTable->unmatchedRows(), false));
    }
}
