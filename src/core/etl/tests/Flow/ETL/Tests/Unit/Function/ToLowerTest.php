<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\lower;
use function Flow\ETL\DSL\row;

final class ToLowerTest extends FlowTestCase
{
    public function test_string_to_lower(): void
    {
        static::assertSame('lower', lower(lit('LOWER'))->eval(row([]), flow_context()));
    }
}
