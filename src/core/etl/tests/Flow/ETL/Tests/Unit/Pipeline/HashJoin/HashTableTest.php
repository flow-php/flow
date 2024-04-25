<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline\HashJoin;

use function Flow\ETL\DSL\{int_entry, refs, row, str_entry};
use Flow\ETL\Hash\PlainText;
use Flow\ETL\Pipeline\HashJoin\HashTable;
use PHPUnit\Framework\TestCase;

final class HashTableTest extends TestCase
{
    public function test_putting_rows_to_buckets() : void
    {
        $hashTable = new HashTable(new PlainText());

        $hashTable->add(row(int_entry('id', 1), str_entry('value', '1')), refs('id'));
        $hashTable->add(row(int_entry('id', 1), str_entry('value', '2')), refs('id'));
        $hashTable->add(row(int_entry('id', 1), str_entry('value', '3')), refs('id'));

        $hashTable->add(row(int_entry('id', 2)), refs('id'));
        $hashTable->add(row(int_entry('id', 2)), refs('id'));

        $hashTable->add(row(int_entry('id', 3), str_entry('value', '1')), refs('id'));
        $hashTable->add(row(int_entry('id', 3), str_entry('value', '2')), refs('id'));
        $hashTable->add(row(int_entry('id', 3), str_entry('value', '1')), refs('id'));

        self::assertCount(3, $hashTable->bucketFor(row(int_entry('id', 1)), refs('id')));
        self::assertCount(1, $hashTable->bucketFor(row(int_entry('id', 2)), refs('id')));
        self::assertCount(2, $hashTable->bucketFor(row(int_entry('id', 3)), refs('id')));
        self::assertNull($hashTable->bucketFor(row(int_entry('id', 4)), refs('id')));
    }

    public function test_using_different_references_to_hash_row() : void
    {
        $hashTable = new HashTable(new PlainText());

        $hashTable->add(row(int_entry('id', 1)), refs('id'));
        $hashTable->add(row(int_entry('id', 1)), refs('id'));

        $hashTable->add(row(int_entry('id', 2), str_entry('value', '1')), refs('id'));
        $hashTable->add(row(int_entry('id', 2), str_entry('value', '2')), refs('id'));

        self::assertCount(1, $hashTable->bucketFor(row(int_entry('identifier', 1)), refs('identifier')));
        self::assertCount(2, $hashTable->bucketFor(row(int_entry('identifier', 2)), refs('identifier')));
    }
}
