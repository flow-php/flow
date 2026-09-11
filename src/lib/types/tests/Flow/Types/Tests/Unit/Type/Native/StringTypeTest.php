<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use DateInterval;
use DateTime;
use DateTimeImmutable;
use DateTimeZone;
use Dom\HTMLDocument;
use DOMDocument;
use DOMElement;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Tests\Unit\Type\Fixtures\StringableObject;
use Flow\Types\Type\Native\StringType;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;
use stdClass;
use Stringable;

use function Flow\Types\DSL\type_from_array;
use function Flow\Types\DSL\type_string;
use function trim;

use const LIBXML_HTML_NOIMPLIED;
use const LIBXML_NOERROR;

final class StringTypeTest extends TestCase
{
    public static function assert_data_provider(): Generator
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
            'value' => new stdClass(),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTimeImmutable' => [
            'value' => new DateTimeImmutable(),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTime' => [
            'value' => new DateTime(),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTimeZone' => [
            'value' => new DateTimeZone('UTC'),
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider(): Generator
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
            'value' => new DateTimeImmutable('2021-01-01 00:00:00'),
            'expected' => '2021-01-01T00:00:00+00:00',
            'exceptionClass' => null,
        ];

        yield 'Stringable' => [
            'value' => new class() implements Stringable {
                public function __toString(): string
                {
                    return 'stringable';
                }
            },
            'expected' => 'stringable',
            'exceptionClass' => null,
        ];

        yield 'DOMDocument' => [
            'value' => new DOMDocument(),
            'expected' => '<?xml version="1.0"?>',
            'exceptionClass' => null,
        ];

        $xml = new DOMDocument();
        $xml->loadXML('<xml>Some Happy XML</xml>');

        yield 'Not Empty DOMDocument' => [
            'value' => $xml,
            'expected' => '<xml>Some Happy XML</xml>',
            'exceptionClass' => null,
        ];

        yield 'DOMElement' => [
            'value' => new DOMElement('element'),
            'expected' => '<element/>',
            'exceptionClass' => null,
        ];

        yield 'DateTimeZone' => [
            'value' => new DateTimeZone('UTC'),
            'expected' => 'UTC',
            'exceptionClass' => null,
        ];
    }

    public static function is_stringable_data_provider(): Generator
    {
        yield 'DateTimeImmutable' => [
            'value' => new DateTimeImmutable(),
            'expected' => false,
        ];

        yield 'DateInterval' => [
            'value' => new DateInterval('P1D'),
            'expected' => false,
        ];

        yield 'StringableObject' => [
            'value' => new StringableObject(),
            'expected' => true,
        ];
    }

    public static function is_valid_data_provider(): Generator
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

    /**
     * @param null|class-string<\Throwable> $exceptionClass
     */
    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, ?string $exceptionClass = null): void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_string()->assert($value);
        } else {
            static::assertIsString(type_string()->assert($value));
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
            type_string()->cast($value);
        } else {
            static::assertSame($expected, trim(type_string()->cast($value)));
        }
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_cast_html_document(): void
    {
        // @mago-expect analysis:unavailable-method
        $element = HTMLDocument::createFromString('<p><span>foobar</span></p>', LIBXML_HTML_NOIMPLIED | LIBXML_NOERROR);

        static::assertSame('<p><span>foobar</span></p>', type_string()->cast($element));
    }

    public function test_null_is_not_an_empty_string(): void
    {
        $this->expectException(CastingException::class);

        type_string()->cast(null);
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_cast_html_element(): void
    {
        // @mago-expect analysis:unavailable-method
        $element = HTMLDocument::createFromString('<p><span>foobar</span></p>', LIBXML_HTML_NOIMPLIED | LIBXML_NOERROR);

        static::assertSame('<span>foobar</span>', type_string()->cast($element->documentElement));
    }

    #[DataProvider('is_stringable_data_provider')]
    public function test_is_stringable(mixed $value, bool $expected): void
    {
        static::assertSame($expected, (new StringType())->isStringable($value));
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected): void
    {
        static::assertSame($expected, type_string()->isValid($value));
    }

    public function test_normalization(): void
    {
        $type = type_string();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        static::assertEquals($type, $recreated);
    }

    public function test_to_string(): void
    {
        static::assertSame('string', type_string()->toString());
    }
}
