<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use DateTimeImmutable;
use DateTimeZone;
use DOMDocument;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;

use function Flow\Types\DSL\type_from_array;
use function Flow\Types\DSL\type_xml;

final class XMLTypeTest extends TestCase
{
    public static function assert_data_provider(): Generator
    {
        yield 'valid DOMDocument' => [
            'value' => new DOMDocument(),
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
            'value' => new stdClass(),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTimeZone' => [
            'value' => new DateTimeZone('UTC'),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTimeImmutable' => [
            'value' => new DateTimeImmutable(),
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider(): Generator
    {
        yield 'string to XML' => [
            'value' => '<items><item>1</item></items>',
            'expected' => '<?xml version="1.0"?>' . "\n" . '<items><item>1</item></items>' . "\n",
            'exceptionClass' => null,
        ];

        yield 'integer to XML' => [
            'value' => 1,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];
    }

    public static function is_valid_data_provider(): Generator
    {
        yield 'valid DOMDocument' => [
            'value' => new DOMDocument(),
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

    /**
     * @param null|class-string<\Throwable> $exceptionClass
     */
    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, ?string $exceptionClass = null): void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_xml()->assert($value);
        } else {
            static::assertInstanceOf(DOMDocument::class, type_xml()->assert($value));
        }
    }

    /**
     * @param null|class-string<\Throwable> $exceptionClass
     */
    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, mixed $expected, ?string $exceptionClass): void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_xml()->cast($value);
        } else {
            $result = type_xml()->cast($value);
            static::assertSame($expected, $result->saveXML());
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected): void
    {
        static::assertSame($expected, type_xml()->isValid($value));
    }

    public function test_normalization(): void
    {
        $type = type_xml();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        static::assertEquals($type, $recreated);
    }

    public function test_to_string(): void
    {
        static::assertSame('xml', type_xml()->toString());
    }
}
