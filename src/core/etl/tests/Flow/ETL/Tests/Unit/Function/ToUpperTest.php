<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\upper;

final class ToUpperTest extends FlowTestCase
{
    public function test_string_to_upper(): void
    {
        static::assertSame('UPPER', (new FunctionContext(flow_context()))->eval(upper(lit('upper')), [], schema()));
    }
}
