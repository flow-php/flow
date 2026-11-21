<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Kernels\Hash;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Kernels\Hash\PhpAlgorithm;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\hash_algorithm;

final class PhpAlgorithmTest extends FlowTestCase
{
    public function test_hashing_string_using_xxh128_by_default(): void
    {
        static::assertSame('6c78e0e3bd51d358d01e758642b85fb8', (new PhpAlgorithm())->hash('test'));
        static::assertSame('6c78e0e3bd51d358d01e758642b85fb8', hash_algorithm()->hash('test'));
    }

    public function test_support_sha512_hash(): void
    {
        static::assertSame(
            'ee26b0dd4af7e749aa1a8ee3c10ae9923f618980772e473f8819a5d4940e0db27ac185f8a0e1d5f84f88bc887fd67b143732c304cc5fa9ad8e6f57f50028a8ff',
            (new PhpAlgorithm('sha512'))->hash('test'),
        );
        static::assertSame(
            'ee26b0dd4af7e749aa1a8ee3c10ae9923f618980772e473f8819a5d4940e0db27ac185f8a0e1d5f84f88bc887fd67b143732c304cc5fa9ad8e6f57f50028a8ff',
            hash_algorithm('sha512')->hash('test'),
        );
    }

    public function test_hash_algorithm_passes_the_binary_flag_and_the_options(): void
    {
        static::assertEquals(new PhpAlgorithm('xxh64', true, ['seed' => 7]), hash_algorithm('xxh64', true, [
            'seed' => 7,
        ]));
    }

    public function test_hashing_unknown_algorithm(): void
    {
        static::expectException(InvalidArgumentException::class);
        static::expectExceptionMessage('Hashing algorithm "unknown" is not supported');

        hash_algorithm('unknown');
    }
}
