<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\upper;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class ToUpperTest extends TestCase
{
    public function test_int_to_upper() : void
    {
        $this->assertSame(
            1,
            upper(lit(1))->eval(Row::create())
        );
    }

    public function test_string_to_upper() : void
    {
        $this->assertSame(
            'UPPER',
            upper(lit('upper'))->eval(Row::create())
        );
    }
}
