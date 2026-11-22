<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Metrics;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Runtime\Metrics\Hash;
use PHPUnit\Framework\TestCase;

final class HashTest extends TestCase
{
    public function test_normalize_writes_the_rows_and_the_value(): void
    {
        static::assertSame(
            ['rows' => 120, 'value' => 'ab8476f29c95212a306bffff897d7afb'],
            (new Hash(120, 'ab8476f29c95212a306bffff897d7afb'))->normalize(),
        );
    }

    public function test_plus_adds_the_rows_and_each_lane_of_the_value_with_wrapping(): void
    {
        static::assertEquals(
            new Hash(5, 'ffffffffffffffff0000000000000000'),
            (new Hash(2, 'ffffffffffffffffffffffffffffffff'))->plus(new Hash(3, '00000000000000000000000000000001')),
        );
    }

    public function test_empty_is_the_identity_of_plus(): void
    {
        $hash = new Hash(120, 'ab8476f29c95212a306bffff897d7afb');

        static::assertEquals($hash, Hash::empty()->plus($hash));
        static::assertEquals(new Hash(0, '00000000000000000000000000000000'), Hash::empty());
    }

    public function test_from_array_reads_back_what_normalize_wrote(): void
    {
        $hash = new Hash(120, 'ab8476f29c95212a306bffff897d7afb');

        static::assertEquals($hash, Hash::fromArray($hash->normalize()));
    }

    public function test_from_array_names_a_missing_key(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A hash needs "rows" of type integer');

        Hash::fromArray(['value' => 'ab8476f29c95212a306bffff897d7afb']);
    }

    public function test_from_array_names_a_key_of_another_type(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A hash needs "value" of type string');

        Hash::fromArray(['rows' => 120, 'value' => 7]);
    }
}
