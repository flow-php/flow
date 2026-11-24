<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Foundation\Hash;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Hash\HashAlgorithm;
use PHPUnit\Framework\TestCase;

final class HashAlgorithmTest extends TestCase
{
    public function test_a_name_gives_its_algorithm(): void
    {
        static::assertSame(HashAlgorithm::sha256, HashAlgorithm::named('sha256'));
    }

    public function test_an_unknown_name_lists_the_algorithms(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unknown hash algorithm "sha1", use one of: md5, sha256, xxh128');

        HashAlgorithm::named('sha1');
    }
}
