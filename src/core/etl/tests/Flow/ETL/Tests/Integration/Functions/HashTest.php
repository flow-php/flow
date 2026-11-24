<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Functions;

use Flow\ETL\Foundation\Hash\HashAlgorithm;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;

final class HashTest extends FlowTestCase
{
    public static function provideValues(): Generator
    {
        yield 'array' => [[1, 2, 3], 'f1c4574435e8e2806215a6b677d5e06b'];
        yield 'string' => ['value', 'd7ab8cce59abd5050d59506fb013961a'];
    }

    #[DataProvider('provideValues')]
    public function test_hash_on_given_value(mixed $value, string $expected): void
    {
        data_frame()
            ->read(from_array([
                ['key' => $value],
            ]))
            ->withColumn('hash', ref('key')->hash())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['key' => $value, 'hash' => $expected],
            ],
            $memory->dump(),
        );
    }

    public function test_hash_with_different_algorithm(): void
    {
        data_frame()
            ->read(from_array([
                ['key' => 'value'],
            ]))
            ->withColumn('hash', ref('key')->hash(HashAlgorithm::sha256))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                [
                    'key' => 'value',
                    'hash' => 'cd42404d52ad55ccfa9aca4adc828aa5800ad9d385a0671fbcbf724118320619',
                ],
            ],
            $memory->dump(),
        );
    }
}
