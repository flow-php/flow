<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\hash;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class HashTest extends TestCase
{
    public function test_hashing_array_value() : void
    {
        $this->assertSame(
            '4450cf82dc53848e2bbe9798b70b0a6a',
            ref('value')->hash()->eval(Row::create(Entry::array('value', ['test']))),
        );
    }

    public function test_hashing_concat() : void
    {
        $this->assertSame(
            \hash('xxh128', 'test_test'),
            hash(concat(ref('value'), lit('_'), ref('value')))->eval(Row::create(Entry::str('value', 'test')))
        );
    }

    public function test_hashing_datetime() : void
    {
        $this->assertSame(
            '5347d10de38eb5570c044eb710a5120a',
            ref('value')->hash()->eval(Row::create(Entry::datetime('value', new \DateTimeImmutable('2021-01-01')))),
        );
    }

    public function test_hashing_null_value() : void
    {
        $this->assertSame(
            null,
            ref('value')->hash()->eval(Row::create(Entry::null('value'))),
        );
    }

    public function test_hashing_string_value() : void
    {
        $this->assertSame(
            '6c78e0e3bd51d358d01e758642b85fb8',
            ref('value')->hash()->eval(Row::create(Entry::str('value', 'test'))),
        );
    }
}
