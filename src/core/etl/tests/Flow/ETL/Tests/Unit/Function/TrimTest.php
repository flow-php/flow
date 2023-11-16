<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Function\Trim\Type;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class TrimTest extends TestCase
{
    public function test_trim_both_valid_string() : void
    {
        $this->assertSame(
            'value',
            ref('string')->trim()->eval(Row::create(Entry::str('string', '   value')))
        );
    }

    public function test_trim_integer() : void
    {
        $this->assertNull(
            ref('integer')->trim()->eval(Row::create(Entry::integer('integer', 1)))
        );
    }

    public function test_trim_left_valid_string() : void
    {
        $this->assertSame(
            'value   ',
            ref('string')->trim(Type::LEFT)->eval(Row::create(Entry::str('string', '   value   ')))
        );
    }

    public function test_trim_right_valid_string() : void
    {
        $this->assertSame(
            '   value',
            ref('string')->trim(Type::RIGHT)->eval(Row::create(Entry::str('string', '   value   ')))
        );
    }
}
