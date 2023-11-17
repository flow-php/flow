<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class HashTest extends TestCase
{
    public static function provideValues() : \Generator
    {
        yield 'array' => [[1, 2, 3], 'f1c4574435e8e2806215a6b677d5e06b'];
        yield 'object' => [new \stdClass(), 'f2ba00ab9bfb5c37e41fed64ffe5ea8a'];
        yield 'string' => ['value', 'd7ab8cce59abd5050d59506fb013961a'];
    }

    /**
     * @dataProvider provideValues
     */
    public function test_hash_on_given_value(mixed $value, string $expected) : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['key' => $value],
                    ]
                )
            )
            ->withEntry('hash', ref('key')->hash())
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['key' => $value, 'hash' => $expected],
            ],
            $memory->data
        );
    }

    public function test_hash_with_different_algorithm() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['key' => 'value'],
                    ]
                )
            )
            ->withEntry('hash', ref('key')->hash('sha512'))
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['key' => 'value', 'hash' => 'ec2c83edecb60304d154ebdb85bdfaf61a92bd142e71c4f7b25a15b9cb5f3c0ae301cfb3569cf240e4470031385348bc296d8d99d09e06b26f09591a97527296'],
            ],
            $memory->data
        );
    }
}
