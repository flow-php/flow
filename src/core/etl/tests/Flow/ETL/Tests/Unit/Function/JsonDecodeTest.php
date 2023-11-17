<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class JsonDecodeTest extends TestCase
{
    public function test_json_decode_expression() : void
    {
        $this->assertSame(
            ['value' => 1],
            ref('value')->jsonDecode()->eval(Row::create(Entry::str('value', '{"value": 1}'))),
        );
    }

    public function test_json_decode_expression_with_invalid_json() : void
    {
        $this->assertNull(
            ref('value')->jsonDecode()->eval(Row::create(Entry::str('value', '{"value": 1'))),
        );
    }

    public function test_json_decode_on_non_json_value() : void
    {
        $this->assertNull(
            ref('value')->jsonDecode()->eval(Row::create(Entry::int('value', 125))),
        );
    }
}
