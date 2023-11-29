<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\str_entry;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class CapitalizeTest extends TestCase
{
    public function test_capitalize_valid_string() : void
    {
        $this->assertSame(
            'This Is A Value',
            ref('string')->capitalize()->eval(Row::create(str_entry('string', 'this is a value')))
        );
    }
}
