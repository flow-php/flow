<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{flow_context, ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class CapitalizeTest extends FlowTestCase
{
    public function test_capitalize_valid_string() : void
    {
        self::assertSame(
            'This Is A Value',
            ref('string')->capitalize()->eval(row(str_entry('string', 'this is a value')), flow_context())
        );
    }
}
