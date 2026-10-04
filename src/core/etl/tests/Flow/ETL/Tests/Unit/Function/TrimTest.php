<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Function\Trim\Type;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class TrimTest extends FlowTestCase
{
    public function test_trim_both_valid_string(): void
    {
        static::assertSame('value', (new FunctionContext(flow_context()))->eval(
            ref('string')->trim(),
            ['string' => '   value'],
            schema(str_schema('string')),
        ));
    }

    public function test_trim_left_valid_string(): void
    {
        static::assertSame('value   ', (new FunctionContext(flow_context()))->eval(
            ref('string')->trim(Type::LEFT),
            ['string' => '   value   '],
            schema(str_schema('string')),
        ));
    }

    public function test_trim_right_valid_string(): void
    {
        static::assertSame('   value', (new FunctionContext(flow_context()))->eval(
            ref('string')->trim(Type::RIGHT),
            ['string' => '   value   '],
            schema(str_schema('string')),
        ));
    }
}
