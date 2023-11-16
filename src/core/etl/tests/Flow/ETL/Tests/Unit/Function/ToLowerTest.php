<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\lower;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class ToLowerTest extends TestCase
{
    public function test_int_to_lower() : void
    {
        $this->assertSame(
            1,
            lower(lit(1))->eval(Row::create())
        );
    }

    public function test_string_to_lower() : void
    {
        $this->assertSame(
            'lower',
            lower(lit('LOWER'))->eval(Row::create())
        );
    }
}
