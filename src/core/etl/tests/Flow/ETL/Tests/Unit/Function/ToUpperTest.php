<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{lit, upper};
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class ToUpperTest extends TestCase
{
    public function test_int_to_upper() : void
    {
        self::assertSame(
            1,
            upper(lit(1))->eval(Row::create())
        );
    }

    public function test_string_to_upper() : void
    {
        self::assertSame(
            'UPPER',
            upper(lit('upper'))->eval(Row::create())
        );
    }
}
