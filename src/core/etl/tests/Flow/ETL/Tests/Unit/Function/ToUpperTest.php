<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{lit, upper};
use Flow\ETL\Tests\FlowTestCase;

final class ToUpperTest extends FlowTestCase
{
    public function test_string_to_upper() : void
    {
        self::assertSame(
            'UPPER',
            upper(lit('upper'))->eval(row())
        );
    }
}
