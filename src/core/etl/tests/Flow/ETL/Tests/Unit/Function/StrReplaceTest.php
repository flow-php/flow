<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class StrReplaceTest extends FlowTestCase
{
    public function test_str_replace_on_valid_string(): void
    {
        static::assertSame('1', (new FunctionContext(flow_context()))->eval(
            ref('value')->strReplace('test', '1'),
            ['value' => 'test'],
            schema(str_schema('value')),
        ));
    }

    public function test_str_replace_on_valid_string_with_array_of_replacements(): void
    {
        static::assertSame('test was successful', (new FunctionContext(flow_context()))->eval(
            ref('value')->strReplace(['is', 'broken'], ['was', 'successful']),
            ['value' => 'test is broken'],
            schema(str_schema('value')),
        ));
    }
}
