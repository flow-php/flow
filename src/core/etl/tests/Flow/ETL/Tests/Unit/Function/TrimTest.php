<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Function\Trim\Type;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class TrimTest extends FlowTestCase
{
    public function test_trim_both_valid_string(): void
    {
        static::assertSame('value', ref('string')->trim()->eval(row(['string' => '   value']), flow_context()));
    }

    public function test_trim_left_valid_string(): void
    {
        static::assertSame('value   ', ref('string')
            ->trim(Type::LEFT)
            ->eval(row(['string' => '   value   ']), flow_context()));
    }

    public function test_trim_right_valid_string(): void
    {
        static::assertSame('   value', ref('string')
            ->trim(Type::RIGHT)
            ->eval(row(['string' => '   value   ']), flow_context()));
    }
}
