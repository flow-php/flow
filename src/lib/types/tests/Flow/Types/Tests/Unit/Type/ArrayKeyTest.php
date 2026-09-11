<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type;

use Flow\Types\Type\ArrayKey;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ArrayKeyTest extends TestCase
{
    #[DataProvider('keys')]
    public function test_coerce_matches_php_array_key_coercion(int|string $key, int|string $expected): void
    {
        static::assertSame($expected, ArrayKey::coerce($key));
    }

    public static function keys(): Generator
    {
        yield 'numeric string' => ['0', 0];
        yield 'negative numeric string' => ['-3', -3];
        yield 'integer' => [5, 5];
        yield 'plain string' => ['b', 'b'];
        yield 'leading zero does not round trip' => ['01', '01'];
        yield 'float string does not round trip' => ['1.5', '1.5'];
        yield 'negative zero does not round trip' => ['-0', '-0'];
    }
}
