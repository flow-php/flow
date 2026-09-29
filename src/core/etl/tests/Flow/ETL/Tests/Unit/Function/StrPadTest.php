<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

use const STR_PAD_LEFT;

final class StrPadTest extends FlowTestCase
{
    public function test_str_pad_on_valid_string(): void
    {
        static::assertSame('----N', (new FunctionContext(flow_context()))->eval(
            ref('value')->strPad(5, '-', STR_PAD_LEFT),
            ['value' => 'N'],
            schema(str_schema('value')),
        ));
    }
}
