<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class CapitalizeTest extends FlowTestCase
{
    public function test_capitalize_valid_string(): void
    {
        static::assertSame('This Is A Value', ref('string')
            ->capitalize()
            ->eval(row(['string' => 'this is a value']), flow_context()));
    }
}
