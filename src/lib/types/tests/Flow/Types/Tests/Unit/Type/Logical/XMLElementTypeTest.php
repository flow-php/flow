<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\{type_from_array, type_xml_element};
use Flow\Types\Exception\{InvalidTypeException};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class XMLElementTypeTest extends TestCase
{
    public static function assert_data_provider() : \Generator
    {
        yield 'valid DOMElement' => [
            'value' => new \DOMElement('xml'),
            'exceptionClass' => null,
        ];

        yield 'invalid string' => [
            'value' => 'string',
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid UUID string' => [
            'value' => '49e952c8-80ec-4910-a1d6-a19bd46b163d',
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid boolean' => [
            'value' => false,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid float' => [
            'value' => 124.25,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid array' => [
            'value' => [1, 2],
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid object' => [
            'value' => new \stdClass(),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTimeZone' => [
            'value' => new \DateTimeZone('UTC'),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTimeImmutable' => [
            'value' => new \DateTimeImmutable(),
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider() : \Generator
    {
        yield 'DOMElement stays as is' => [
            'value' => $element = new \DOMElement('xml'),
            'expected' => $element,
            'exceptionClass' => null,
        ];

        yield 'string to DOMElement' => [
            'value' => '<xml></xml>',
            'expected' => new \DOMElement('xml'),
            'exceptionClass' => null,
        ];
    }

    public static function is_valid_data_provider() : \Generator
    {
        yield 'valid DOMElement' => [
            'value' => new \DOMElement('xml'),
            'expected' => true,
        ];

        yield 'invalid XML string' => [
            'value' => '<xml></xml>',
            'expected' => false,
        ];

        yield 'invalid date string' => [
            'value' => '2020-01-01',
            'expected' => false,
        ];

        yield 'invalid datetime string' => [
            'value' => '2020-01-01 00:00:00',
            'expected' => false,
        ];
    }

    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, ?string $exceptionClass = null) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_xml_element()->assert($value);
        } else {
            self::assertInstanceOf(\DOMElement::class, type_xml_element()->assert($value));
        }
    }

    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, mixed $expected, ?string $exceptionClass) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_xml_element()->cast($value);
        } else {
            $result = type_xml_element()->cast($value);

            if ($result instanceof \DOMElement && $expected instanceof \DOMElement) {
                self::assertEquals($expected->nodeName, $result->nodeName);
            } else {
                self::assertSame($expected, $result);
            }
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected) : void
    {
        self::assertSame($expected, type_xml_element()->isValid($value));
    }

    public function test_normalization() : void
    {
        $type = type_xml_element();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'xml_element',
            type_xml_element()->toString()
        );
    }
}
