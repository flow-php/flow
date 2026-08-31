<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\optional;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class OptionalTest extends FlowTestCase
{
    public function test_optional_declares_the_inner_type_widened_to_optional(): void
    {
        static::assertSame('?string', optional(ref('name')->upper())->returns()->toString());
    }

    public function test_optional_returns_null_when_the_inner_function_throws(): void
    {
        static::assertNull(optional(ref('name')->upper())->eval(row(['other' => 'flow']), flow_context()));
    }
}
