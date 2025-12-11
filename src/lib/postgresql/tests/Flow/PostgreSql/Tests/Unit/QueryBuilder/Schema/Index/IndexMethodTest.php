<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Index;

use Flow\PostgreSql\QueryBuilder\Schema\Index\IndexMethod;
use PHPUnit\Framework\TestCase;

final class IndexMethodTest extends TestCase
{
    public function test_all_index_methods_exist() : void
    {
        $cases = IndexMethod::cases();

        self::assertCount(6, $cases);
        self::assertContains(IndexMethod::BTREE, $cases);
        self::assertContains(IndexMethod::HASH, $cases);
        self::assertContains(IndexMethod::GIST, $cases);
        self::assertContains(IndexMethod::SPGIST, $cases);
        self::assertContains(IndexMethod::GIN, $cases);
        self::assertContains(IndexMethod::BRIN, $cases);
    }

    public function test_brin_value() : void
    {
        self::assertSame('brin', IndexMethod::BRIN->value);
    }

    public function test_btree_value() : void
    {
        self::assertSame('btree', IndexMethod::BTREE->value);
    }

    public function test_gin_value() : void
    {
        self::assertSame('gin', IndexMethod::GIN->value);
    }

    public function test_gist_value() : void
    {
        self::assertSame('gist', IndexMethod::GIST->value);
    }

    public function test_hash_value() : void
    {
        self::assertSame('hash', IndexMethod::HASH->value);
    }

    public function test_spgist_value() : void
    {
        self::assertSame('spgist', IndexMethod::SPGIST->value);
    }
}
