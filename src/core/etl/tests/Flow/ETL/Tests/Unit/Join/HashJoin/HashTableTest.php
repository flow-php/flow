<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\HashJoin;

use Flow\ETL\Join\HashJoin\HashTable;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

final class HashTableTest extends FlowTestCase
{
    public function test_candidates_for_matching_hash(): void
    {
        $hashTable = new HashTable();
        $hashTable->add('hash-a', array_to_row(
            ['user_id' => 1, 'name' => 'Alice'],
            schema(int_schema('user_id'), str_schema('name')),
        ));
        $hashTable->add('hash-b', array_to_row(
            ['user_id' => 2, 'name' => 'Bob'],
            schema(int_schema('user_id'), str_schema('name')),
        ));

        $candidates = $hashTable->candidatesFor('hash-a');

        static::assertCount(1, $candidates);
        static::assertSame('Alice', $candidates[0]->get('name'));
    }

    public function test_constant_hash_makes_every_row_a_candidate(): void
    {
        $hashTable = new HashTable();
        $hashTable->add('0', array_to_row(['user_id' => 1], schema(int_schema('user_id'))));
        $hashTable->add('0', array_to_row(['user_id' => 2], schema(int_schema('user_id'))));

        static::assertCount(2, $hashTable->candidatesFor('0'));
    }

    public function test_duplicated_rows_are_preserved(): void
    {
        $hashTable = new HashTable();
        $hashTable->add('hash-a', array_to_row(
            ['user_id' => 1, 'name' => 'Alice'],
            schema(int_schema('user_id'), str_schema('name')),
        ));
        $hashTable->add('hash-a', array_to_row(
            ['user_id' => 1, 'name' => 'Alice'],
            schema(int_schema('user_id'), str_schema('name')),
        ));

        static::assertSame(2, $hashTable->count());
        static::assertCount(2, $hashTable->candidatesFor('hash-a'));
    }

    public function test_no_candidates_for_unknown_hash(): void
    {
        $hashTable = new HashTable();
        $hashTable->add('hash-a', array_to_row(['user_id' => 1], schema(int_schema('user_id'))));

        static::assertSame([], $hashTable->candidatesFor('hash-404'));
    }

    public function test_unmatched_rows_tracking(): void
    {
        $hashTable = new HashTable(trackUnmatched: true);
        $hashTable->add('hash-a', array_to_row(
            ['user_id' => 1, 'name' => 'Alice'],
            schema(int_schema('user_id'), str_schema('name')),
        ));
        $hashTable->add('hash-b', array_to_row(
            ['user_id' => 2, 'name' => 'Bob'],
            schema(int_schema('user_id'), str_schema('name')),
        ));
        $hashTable->add('hash-c', array_to_row(
            ['user_id' => 3, 'name' => 'Cid'],
            schema(int_schema('user_id'), str_schema('name')),
        ));

        $hashTable->matched(1);

        $unmatched = iterator_to_array($hashTable->unmatchedRows(), false);

        static::assertCount(2, $unmatched);
        static::assertSame('Alice', $unmatched[0]->get('name'));
        static::assertSame('Cid', $unmatched[1]->get('name'));
    }

    public function test_unmatched_rows_without_tracking_are_empty(): void
    {
        $hashTable = new HashTable();
        $hashTable->add('hash-a', array_to_row(['user_id' => 1], schema(int_schema('user_id'))));

        static::assertSame([], iterator_to_array($hashTable->unmatchedRows(), false));
    }
}
