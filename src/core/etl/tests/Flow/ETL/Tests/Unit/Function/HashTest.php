<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeImmutable;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\hash;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class HashTest extends FlowTestCase
{
    public function test_hashing_array_value(): void
    {
        static::assertSame('4450cf82dc53848e2bbe9798b70b0a6a', ref('value')
            ->hash()
            ->eval(row(json_entry('value', ['test'])), flow_context()));
    }

    public function test_hashing_concat(): void
    {
        static::assertSame(
            NativePHPHash::xxh128('test_test'),
            hash(concat(ref('value'), lit('_'), ref('value')), new NativePHPHash('xxh128'))->eval(
                row(str_entry('value', 'test')),
                flow_context(),
            ),
        );
    }

    public function test_hashing_datetime(): void
    {
        static::assertSame('5347d10de38eb5570c044eb710a5120a', ref('value')
            ->hash()
            ->eval(row(datetime_entry('value', new DateTimeImmutable('2021-01-01'))), flow_context()));
    }

    public function test_hashing_null_value(): void
    {
        static::assertNull(ref('value')->hash()->eval(row(str_entry('value', null)), flow_context()));
    }

    public function test_hashing_string_value(): void
    {
        static::assertSame('6c78e0e3bd51d358d01e758642b85fb8', ref('value')
            ->hash()
            ->eval(row(str_entry('value', 'test')), flow_context()));
    }
}
