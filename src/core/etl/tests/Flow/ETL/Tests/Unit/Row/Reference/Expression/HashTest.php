<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Rowreference\Expression;

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
            '18f4cf4cf6ad53618cd47d604ef6a6f9c0cbc6755d6caa31f333e851b3671328',
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
            'c7db551aff37ffed2f35e6a5b8449499bef83a5036f84847f03f9c9eedadb05e',
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
            '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08',
            ref('value')->hash()->eval(Row::create(Entry::str('value', 'test'))),
        );
    }
}
