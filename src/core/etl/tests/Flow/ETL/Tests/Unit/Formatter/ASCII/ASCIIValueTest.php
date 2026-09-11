<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Formatter\ASCII;

use DateTimeImmutable;
use Flow\ETL\Formatter\ASCII\ASCIIValue;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_string;

final class ASCIIValueTest extends FlowTestCase
{
    public static function values_with_truncating(): Generator
    {
        yield [type_string(), 'string', 'str'];
        yield [type_boolean(), false, 'fal'];
        yield [type_boolean(), true, 'tru'];
        yield [type_datetime(), new DateTimeImmutable('2023-01-01 00:00:00 UTC'), '202'];
        yield [type_json(), ['a' => 1, 'b' => 2, 'c' => ['test']], '{"a'];
    }

    public static function values_without_truncating(): Generator
    {
        yield [type_string(), 'string', 'string'];
        yield [type_integer(), 1, '1'];
        yield [type_boolean(), false, 'false'];
        yield [type_boolean(), true, 'true'];
        yield [type_datetime(), new DateTimeImmutable('2023-01-01 00:00:00 UTC'), '2023-01-01T00:00:00+00:00'];
        yield [type_json(), ['a' => 1, 'b' => 2, 'c' => ['test']], '{"a":1,"b":2,"c":["test"]}'];
    }

    /**
     * @param Type<mixed> $type
     */
    #[DataProvider('values_without_truncating')]
    public function test_converting_value_to_ascii_value(Type $type, mixed $value, string $asciiValue): void
    {
        static::assertSame($asciiValue, (new ASCIIValue($type, $value))->print(false));
    }

    /**
     * @param Type<mixed> $type
     */
    #[DataProvider('values_with_truncating')]
    public function test_converting_value_to_ascii_value_with_truncating(
        Type $type,
        mixed $value,
        string $asciiValue,
    ): void {
        static::assertSame($asciiValue, (new ASCIIValue($type, $value))->print(3));
    }

    public function test_mb_str_pad(): void
    {
        static::assertSame('00ąćę', ASCIIValue::mb_str_pad('ąćę', 5, '0', STR_PAD_LEFT));

        static::assertSame('ąćę00', ASCIIValue::mb_str_pad('ąćę', 5, '0', STR_PAD_RIGHT));

        static::assertSame('0ąćę0', ASCIIValue::mb_str_pad('ąćę', 5, '0', STR_PAD_BOTH));
    }
}
