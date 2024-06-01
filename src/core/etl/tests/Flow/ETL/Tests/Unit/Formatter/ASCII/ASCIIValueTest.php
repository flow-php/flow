<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Formatter\ASCII;

use function Flow\ETL\DSL\datetime_entry;
use Flow\ETL\Formatter\ASCII\ASCIIValue;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ASCIIValueTest extends TestCase
{
    public static function values_with_truncating() : \Generator
    {
        yield ['string', 'str'];
        yield [false, 'fal'];
        yield [true, 'tru'];
        yield [datetime_entry('test', new \DateTimeImmutable('2023-01-01 00:00:00 UTC')), '202'];
        yield [['a' => 1, 'b' => 2, 'c' => ['test']], '{"a'];
    }

    public static function values_without_truncating() : \Generator
    {
        yield ['string', 'string'];
        yield [1, '1'];
        yield [false, 'false'];
        yield [true, 'true'];
        yield [datetime_entry('test', new \DateTimeImmutable('2023-01-01 00:00:00 UTC')), '2023-01-01T00:00:00+00:00'];
        yield [['a' => 1, 'b' => 2, 'c' => ['test']], '{"a":1,"b":2,"c":["test"]}'];
    }

    #[DataProvider('values_without_truncating')]
    public function test_converting_value_to_ascii_value(mixed $value, string $asciiValue) : void
    {
        self::assertSame(
            $asciiValue,
            (new ASCIIValue($value))->print(false),
        );
    }

    #[DataProvider('values_with_truncating')]
    public function test_converting_value_to_ascii_value_with_truncating(mixed $value, string $asciiValue) : void
    {
        self::assertSame(
            $asciiValue,
            (new ASCIIValue($value))->print(3),
        );
    }

    public function test_mb_str_pad() : void
    {
        self::assertSame(
            '00ąćę',
            ASCIIValue::mb_str_pad('ąćę', 5, '0', STR_PAD_LEFT)
        );

        self::assertSame(
            'ąćę00',
            ASCIIValue::mb_str_pad('ąćę', 5, '0', STR_PAD_RIGHT)
        );

        self::assertSame(
            '0ąćę0',
            ASCIIValue::mb_str_pad('ąćę', 5, '0', STR_PAD_BOTH)
        );
    }
}
