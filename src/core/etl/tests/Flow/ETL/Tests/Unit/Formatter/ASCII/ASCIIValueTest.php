<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Formatter\ASCII;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Formatter\ASCII\ASCIIValue;
use PHPUnit\Framework\TestCase;

final class ASCIIValueTest extends TestCase
{
    /**
     * @dataProvider values_without_truncating
     */
    public function test_converting_value_to_ascii_value(mixed $value, string $asciiValue) : void
    {
        $this->assertSame(
            $asciiValue,
            (new ASCIIValue($value))->print(false),
        );
    }

    /**
     * @dataProvider values_with_truncating
     */
    public function test_converting_value_to_ascii_value_with_truncating(mixed $value, string $asciiValue) : void
    {
        $this->assertSame(
            $asciiValue,
            (new ASCIIValue($value))->print(3),
        );
    }

    public function test_mb_str_pad() : void
    {
        $this->assertSame(
            '00ąćę',
            ASCIIValue::mb_str_pad('ąćę', 5, '0', STR_PAD_LEFT)
        );

        $this->assertSame(
            'ąćę00',
            ASCIIValue::mb_str_pad('ąćę', 5, '0', STR_PAD_RIGHT)
        );

        $this->assertSame(
            '0ąćę0',
            ASCIIValue::mb_str_pad('ąćę', 5, '0', STR_PAD_BOTH)
        );
    }

    protected function values_with_truncating() : \Generator
    {
        yield ['string', 'str'];
        yield [false, 'fal'];
        yield [true, 'tru'];
        yield [Entry::datetime('test', new \DateTimeImmutable('2023-01-01 00:00:00 UTC')), '202'];
        yield [['a' => 1, 'b' => 2, 'c' => ['test']], '{"a'];
    }

    protected function values_without_truncating() : \Generator
    {
        yield ['string', 'string'];
        yield [1, '1'];
        yield [false, 'false'];
        yield [true, 'true'];
        yield [Entry::datetime('test', new \DateTimeImmutable('2023-01-01 00:00:00 UTC')), '2023-01-01T00:00:00+00:00'];
        yield [['a' => 1, 'b' => 2, 'c' => ['test']], '{"a":1,"b":2,"c":["test"]}'];
    }
}
