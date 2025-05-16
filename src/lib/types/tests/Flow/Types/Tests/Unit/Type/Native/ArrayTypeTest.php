<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use function Flow\Types\DSL\type_array;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class ArrayTypeTest extends TestCase
{
    public function test_casting_boolean_to_array() : void
    {
        self::assertEquals(
            [true],
            type_array()->cast(true)
        );
    }

    public function test_casting_datetime_to_array() : void
    {
        self::assertEquals(
            ['date' => '2021-01-01 00:00:00.000000', 'timezone_type' => 3, 'timezone' => 'UTC'],
            type_array()->cast(new \DateTimeImmutable('2021-01-01 00:00:00 UTC'))
        );
    }

    public function test_casting_float_to_array() : void
    {
        self::assertEquals(
            [1.1],
            type_array()->cast(1.1)
        );
    }

    public function test_casting_integer_to_array() : void
    {
        self::assertEquals(
            [1],
            type_array()->cast(1)
        );
    }

    public function test_casting_string_to_array() : void
    {
        self::assertSame(
            ['items' => ['item' => 1]],
            type_array()->cast('{"items":{"item":1}}')
        );
    }

    public function test_casting_xml_document_to_array() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML($xmlString = '<root><foo baz="buz">bar</foo></root>');

        self::assertSame(
            ['root' => ['foo' => ['@attributes' => ['baz' => 'buz'], '@value' => 'bar']]],
            type_array()->cast($xml)
        );
    }

    #[TestWith(['[]', 'Expected type "array<mixed>", got "json"'])]
    public function test_invalid_assertion(mixed $value, string $exception) : void
    {
        $this->expectExceptionMessage($exception);

        type_array()->assert($value);
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'array<mixed>',
            type_array()->toString()
        );
    }

    public function test_valid() : void
    {
        self::assertTrue(
            type_array()->isValid([])
        );
        self::assertTrue(
            type_array()->isValid(['one'])
        );
        self::assertTrue(
            type_array()->isValid([1])
        );
        self::assertFalse(
            type_array()->isValid(null)
        );
        self::assertFalse(
            type_array()->isValid('one')
        );
        self::assertFalse(
            type_array()->isValid(true)
        );
        self::assertFalse(
            type_array()->isValid(123)
        );
    }

    #[TestWith([[1]])]
    #[TestWith([['a' => 'b']])]
    #[TestWith([[]])]
    public function test_valid_assertion(array $value) : void
    {
        /** @phpstan-ignore-next-line */
        self::assertIsArray(type_array()->assert($value));
    }
}
