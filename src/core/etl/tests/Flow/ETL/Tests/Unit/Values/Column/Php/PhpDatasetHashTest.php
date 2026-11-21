<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Column\Php;

use Flow\ETL\Values\Column\MultisetDigest;
use Flow\ETL\Values\Column\Php\PhpDatasetHash;
use Flow\ETL\Values\Column\PhpBackend;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function hash;
use function unpack;

final class PhpDatasetHashTest extends TestCase
{
    public function test_one_row_equals_its_row_hash(): void
    {
        $rows = array_to_rows([['a' => 1]], schema(int_schema('a')));
        $hash = new PhpDatasetHash();
        $hash->add($rows->columns(), 1);

        static::assertSame('8e18789eac90baf2a1f6bc7cceafa11a', $hash->digest()->hex());
        static::assertSame(
            (new PhpBackend())
                ->rowHashes($rows->columns(), 1)
                ->physicals(),
            [$hash->digest()->hex()],
        );
    }

    public function test_row_order_does_not_count(): void
    {
        $forward = new PhpDatasetHash();
        $forward->add(array_to_rows([['a' => 1], ['a' => 2]], schema(int_schema('a')))->columns(), 2);
        $backward = new PhpDatasetHash();
        $backward->add(array_to_rows([['a' => 2]], schema(int_schema('a')))->columns(), 1);
        $backward->add(array_to_rows([['a' => 1]], schema(int_schema('a')))->columns(), 1);

        static::assertSame('721f4650154ebb88ac4ec50a605901df', $forward->digest()->hex());
        static::assertEquals($forward->digest(), $backward->digest());
    }

    public function test_a_duplicate_row_counts(): void
    {
        $hash = new PhpDatasetHash();
        $hash->add(array_to_rows([['a' => 1], ['a' => 1]], schema(int_schema('a')))->columns(), 2);

        static::assertSame('1c30f13d592175e443ed78f99d5f4234', $hash->digest()->hex());
    }

    public function test_a_row_of_only_nulls_counts_as_a_row(): void
    {
        $hash = new PhpDatasetHash();
        $hash->add(array_to_rows([['a' => null]], schema(int_schema('a', true)))->columns(), 1);

        static::assertSame('99aa06d3014798d86001c324468d497f', $hash->digest()->hex());
    }

    public function test_a_batch_without_columns_hashes_each_row_as_the_empty_row(): void
    {
        /** @var array{1: int, 2: int} $lanes */
        $lanes = unpack('J2', hash('xxh128', '', true));
        $empty = new MultisetDigest($lanes[2], $lanes[1]);
        $nulls = new PhpDatasetHash();
        $nulls->add(array_to_rows([['a' => null], ['a' => null]], schema(int_schema('a', true)))->columns(), 2);
        $hash = new PhpDatasetHash();
        $hash->add([], 2);

        static::assertEquals($empty->plus($empty), $hash->digest());
        static::assertEquals($nulls->digest(), $hash->digest());
    }

    public function test_an_empty_hash_is_zero(): void
    {
        static::assertEquals(MultisetDigest::empty(), (new PhpDatasetHash())->digest());
    }

    public function test_absorb_adds_to_the_rows_already_hashed(): void
    {
        $hash = new PhpDatasetHash();
        $hash->add(array_to_rows([['a' => 1]], schema(int_schema('a')))->columns(), 1);
        $hash->absorb(new MultisetDigest(745355142457024709, -2015697621351530346));

        static::assertSame('721f4650154ebb88ac4ec50a605901df', $hash->digest()->hex());
    }
}
