<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Hash;

use Flow\ETL\Hash\NativePHPHash;
use PHPUnit\Framework\TestCase;

class NativePHPHashTest extends TestCase
{
    public static function test_hashing_xxh128_by_static_call() : void
    {
        static::assertSame(
            '6c78e0e3bd51d358d01e758642b85fb8',
            NativePHPHash::xxh128('test'),
        );
    }

    public function test_hashing_string_using_xxh128_by_default() : void
    {
        static::assertSame(
            '6c78e0e3bd51d358d01e758642b85fb8',
            NativePHPHash::xxh128('test'),
        );
    }

    public function test_support_sha512_hash() : void
    {
        static::assertSame(
            'ee26b0dd4af7e749aa1a8ee3c10ae9923f618980772e473f8819a5d4940e0db27ac185f8a0e1d5f84f88bc887fd67b143732c304cc5fa9ad8e6f57f50028a8ff',
            (new NativePHPHash('sha512'))->hash('test')
        );

    }
}
