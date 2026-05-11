<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Index;

use Flow\PostgreSql\QueryBuilder\Schema\Index\IndexMethod;
use PHPUnit\Framework\TestCase;

final class IndexMethodTest extends TestCase
{
    public function test_all_index_methods_exist(): void
    {
        $cases = IndexMethod::cases();

        static::assertCount(6, $cases);
        static::assertContains(IndexMethod::BTREE, $cases);
        static::assertContains(IndexMethod::HASH, $cases);
        static::assertContains(IndexMethod::GIST, $cases);
        static::assertContains(IndexMethod::SPGIST, $cases);
        static::assertContains(IndexMethod::GIN, $cases);
        static::assertContains(IndexMethod::BRIN, $cases);
    }

    public function test_brin_value(): void
    {
        static::assertSame('brin', IndexMethod::BRIN->value);
    }

    public function test_btree_value(): void
    {
        static::assertSame('btree', IndexMethod::BTREE->value);
    }

    public function test_gin_value(): void
    {
        static::assertSame('gin', IndexMethod::GIN->value);
    }

    public function test_gist_value(): void
    {
        static::assertSame('gist', IndexMethod::GIST->value);
    }

    public function test_hash_value(): void
    {
        static::assertSame('hash', IndexMethod::HASH->value);
    }

    public function test_spgist_value(): void
    {
        static::assertSame('spgist', IndexMethod::SPGIST->value);
    }
}
