<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class SanitizeTest extends TestCase
{
    public function test_sanitize_on_non_string_value() : void
    {
        $this->assertNull(
            ref('value')->sanitize()->eval(Row::create(Entry::int('value', 1000))),
        );
    }

    public function test_sanitize_on_valid_string() : void
    {
        $this->assertSame(
            '****',
            ref('value')->sanitize()->eval(Row::create(Entry::str('value', 'test'))),
        );
    }

    public function test_sanitize_on_valid_string_with_left_characters() : void
    {
        $this->assertSame(
            'te**',
            ref('value')->sanitize(skipCharacters: lit(2))->eval(Row::create(Entry::str('value', 'test'))),
        );
    }

    public function test_sanitize_on_valid_string_with_left_characters_longer_than_string() : void
    {
        $this->assertSame(
            '****',
            ref('value')->sanitize(skipCharacters: lit(5))->eval(Row::create(Entry::str('value', 'test'))),
        );
    }

    public function test_sanitize_on_valid_string_with_placeholder() : void
    {
        $this->assertSame(
            '----',
            ref('value')->sanitize(lit('-'))->eval(Row::create(Entry::str('value', 'test'))),
        );
    }
}
