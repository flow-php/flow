<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\upper;

final class ToUpperTest extends FlowTestCase
{
    public function test_string_to_upper(): void
    {
        static::assertSame('UPPER', upper(lit('upper'))->eval(row(), flow_context()));
    }
}
