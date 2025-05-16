<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\type_json;
use Flow\ETL\Exception\{CastingException, InvalidTypeException};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class JsonTypeTest extends TestCase
{
    public static function invalid_assert_data_provider() : \Generator
    {
        yield ['string'];
        yield ['49e952c8-80ec-4910-a1d6-a19bd46b163d'];
        yield [false];
        yield [124.25];
        yield [[1, 2]];
        yield [new \stdClass()];
        yield [new \DateTimeZone('UTC')];
    }

    public static function successful_assert_data_provider() : \Generator
    {
        yield ['[1, 2]'];
        yield ['{"foo": "bar"}'];
    }

    public function test_casting_array_to_json() : void
    {
        self::assertSame(
            '{"items":{"item":1}}',
            type_json()->cast(['items' => ['item' => 1]])
        );
    }

    public function test_casting_datetime_to_json() : void
    {
        self::assertSame(
            '{"date":"2021-01-01 00:00:00.000000","timezone_type":3,"timezone":"UTC"}',
            type_json()->cast(new \DateTimeImmutable('2021-01-01 00:00:00 UTC'))
        );
    }

    public function test_casting_integer_to_json() : void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage('Can\'t cast "int" into "json" type');

        type_json()->cast(1);
    }

    public function test_casting_json_string_to_json() : void
    {
        self::assertSame(
            '{"items":{"item":1}}',
            type_json()->cast('{"items":{"item":1}}')
        );
    }

    public function test_casting_non_json_string_to_json() : void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage('Can\'t cast "string" into "json" type');

        type_json()->cast('string');
    }

    #[DataProvider('invalid_assert_data_provider')]
    public function test_invalid_assert(mixed $value) : void
    {
        $this->expectException(InvalidTypeException::class);
        (type_json())->assert($value);
    }

    public function test_is_valid() : void
    {
        self::assertTrue(type_json()->isValid('{"foo": "bar"}'));
        self::assertFalse(type_json()->isValid('{"foo": "bar"'));
        self::assertFalse(type_json()->isValid('2'));
    }

    #[DataProvider('successful_assert_data_provider')]
    public function test_successful_assert(mixed $value) : void
    {
        /** @phpstan-ignore-next-line */
        self::assertIsString(type_json()->assert($value));
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'json',
            type_json()->toString()
        );
    }
}
