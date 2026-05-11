<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor\HashJoin;

use Flow\ETL\Hash\PlainText;
use Flow\ETL\Processor\HashJoin\HashTable;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class HashTableTest extends FlowTestCase
{
    public function test_putting_rows_to_buckets(): void
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

        $bucket1 = $hashTable->bucketFor(row(int_entry('id', 1)), refs('id'));
        static::assertNotNull($bucket1);
        static::assertCount(3, $bucket1);

        $bucket2 = $hashTable->bucketFor(row(int_entry('id', 2)), refs('id'));
        static::assertNotNull($bucket2);
        static::assertCount(1, $bucket2);

        $bucket3 = $hashTable->bucketFor(row(int_entry('id', 3)), refs('id'));
        static::assertNotNull($bucket3);
        static::assertCount(2, $bucket3);
        static::assertNull($hashTable->bucketFor(row(int_entry('id', 4)), refs('id')));
    }

    public function test_using_different_references_to_hash_row(): void
    {
        $hashTable = new HashTable(new PlainText());

        $hashTable->add(row(int_entry('id', 1)), refs('id'));
        $hashTable->add(row(int_entry('id', 1)), refs('id'));

        $hashTable->add(row(int_entry('id', 2), str_entry('value', '1')), refs('id'));
        $hashTable->add(row(int_entry('id', 2), str_entry('value', '2')), refs('id'));

        $bucket4 = $hashTable->bucketFor(row(int_entry('identifier', 1)), refs('identifier'));
        static::assertNotNull($bucket4);
        static::assertCount(1, $bucket4);

        $bucket5 = $hashTable->bucketFor(row(int_entry('identifier', 2)), refs('identifier'));
        static::assertNotNull($bucket5);
        static::assertCount(2, $bucket5);
    }
}
