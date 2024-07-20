<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Flow;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class HashTest extends TestCase
{
    public static function provideValues() : \Generator
    {
        yield 'array' => [[1, 2, 3], 'f1c4574435e8e2806215a6b677d5e06b'];
        yield 'object' => [new \stdClass(), 'f2ba00ab9bfb5c37e41fed64ffe5ea8a'];
        yield 'string' => ['value', 'd7ab8cce59abd5050d59506fb013961a'];
    }

    #[DataProvider('provideValues')]
    public function test_hash_on_given_value(mixed $value, string $expected) : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['key' => $value],
                    ]
                )
            )
            ->withEntry('hash', ref('key')->hash())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['key' => $value, 'hash' => $expected],
            ],
            $memory->dump()
        );
    }

    public function test_hash_with_different_algorithm() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['key' => 'value'],
                    ]
                )
            )
            ->withEntry('hash', ref('key')->hash(new NativePHPHash('sha512')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['key' => 'value', 'hash' => 'ec2c83edecb60304d154ebdb85bdfaf61a92bd142e71c4f7b25a15b9cb5f3c0ae301cfb3569cf240e4470031385348bc296d8d99d09e06b26f09591a97527296'],
            ],
            $memory->dump()
        );
    }
}
