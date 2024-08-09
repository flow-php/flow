<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{lit, lower};
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class ToLowerTest extends TestCase
{
    public function test_string_to_lower() : void
    {
        self::assertSame(
            'lower',
            lower(lit('LOWER'))->eval(Row::create())
        );
    }
}
