<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use function Flow\Types\DSL\{type_from_array, type_string};
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Tests\Unit\Type\Fixtures\StringableObject;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class StringTypeTest extends TestCase
{
    public static function assert_data_provider() : \Generator
    {
        yield 'valid string 1234' => [
            'value' => '1234',
            'exceptionClass' => null,
        ];

        yield 'valid string abcd' => [
            'value' => 'abcd',
            'exceptionClass' => null,
        ];

        yield 'invalid null' => [
            'value' => null,
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

        yield 'invalid DateTimeImmutable' => [
            'value' => new \DateTimeImmutable(),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTime' => [
            'value' => new \DateTime(),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTimeZone' => [
            'value' => new \DateTimeZone('UTC'),
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider() : \Generator
    {
        yield 'string' => [
            'value' => 'string',
            'expected' => 'string',
            'exceptionClass' => null,
        ];

        yield 'int' => [
            'value' => 1,
            'expected' => '1',
            'exceptionClass' => null,
        ];

        yield 'float' => [
            'value' => 1.1,
            'expected' => '1.1',
            'exceptionClass' => null,
        ];

        yield 'bool' => [
            'value' => true,
            'expected' => 'true',
            'exceptionClass' => null,
        ];

        yield 'array' => [
            'value' => [1, 2, 3],
            'expected' => '[1,2,3]',
            'exceptionClass' => null,
        ];

        yield 'DateTimeInterface' => [
            'value' => new \DateTimeImmutable('2021-01-01 00:00:00'),
            'expected' => '2021-01-01T00:00:00+00:00',
            'exceptionClass' => null,
        ];

        yield 'Stringable' => [
            'value' => new class() implements \Stringable {
                public function __toString() : string
                {
                    return 'stringable';
                }
            },
            'expected' => 'stringable',
            'exceptionClass' => null,
        ];

        yield 'DOMDocument' => [
            'value' => new \DOMDocument(),
            'expected' => '<?xml version="1.0"?>',
            'exceptionClass' => null,
        ];

        $xml = (new \DOMDocument());
        $xml->loadXML('<xml>Some Happy XML</xml>');

        yield 'Not Empty DOMDocument' => [
            'value' => $xml,
            'expected' => '<xml>Some Happy XML</xml>',
            'exceptionClass' => null,
        ];

        yield 'DOMElement' => [
            'value' => new \DOMElement('element'),
            'expected' => '<element/>',
            'exceptionClass' => null,
        ];

        yield 'DateTimeZone' => [
            'value' => new \DateTimeZone('UTC'),
            'expected' => 'UTC',
            'exceptionClass' => null,
        ];
    }

    public static function is_stringable_data_provider() : \Generator
    {
        yield 'DateTimeImmutable' => [
            'value' => new \DateTimeImmutable(),
            'expected' => false,
        ];

        yield 'DateInterval' => [
            'value' => new \DateInterval('P1D'),
            'expected' => false,
        ];

        yield 'StringableObject' => [
            'value' => new StringableObject(),
            'expected' => true,
        ];
    }

    public static function is_valid_data_provider() : \Generator
    {
        yield 'valid string' => [
            'value' => 'string',
            'expected' => true,
        ];

        yield 'valid empty string' => [
            'value' => '',
            'expected' => true,
        ];

        yield 'invalid null' => [
            'value' => null,
            'expected' => false,
        ];

        yield 'invalid boolean' => [
            'value' => true,
            'expected' => false,
        ];

        yield 'invalid integer' => [
            'value' => 123,
            'expected' => false,
        ];

        yield 'invalid array' => [
            'value' => [1, 2],
            'expected' => false,
        ];
    }

    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, ?string $exceptionClass = null) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_string()->assert($value);
        } else {
            self::assertIsString(type_string()->assert($value));
        }
    }

    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, mixed $expected, ?string $exceptionClass) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_string()->cast($value);
        } else {
            self::assertSame($expected, \trim(type_string()->cast($value)));
        }
    }

    #[DataProvider('is_stringable_data_provider')]
    public function test_is_stringable(mixed $value, bool $expected) : void
    {
        self::assertSame($expected, type_string()->isStringable($value));
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected) : void
    {
        self::assertSame($expected, type_string()->isValid($value));
    }

    public function test_normalization() : void
    {
        $type = type_string();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'string',
            type_string()->toString()
        );
    }
}
