<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Column;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Values\Column\MultisetHashAlgorithm;
use PHPUnit\Framework\TestCase;

final class MultisetHashAlgorithmTest extends TestCase
{
    public function test_a_name_gives_its_algorithm(): void
    {
        static::assertSame(MultisetHashAlgorithm::sha256, MultisetHashAlgorithm::named('sha256'));
    }

    public function test_an_unknown_name_lists_the_algorithms(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unknown multiset hash algorithm "sha1", use one of: md5, sha256, xxh128');

        MultisetHashAlgorithm::named('sha1');
    }
}
