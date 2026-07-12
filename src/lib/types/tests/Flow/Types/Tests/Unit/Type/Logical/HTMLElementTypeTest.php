<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use DateTimeImmutable;
use DateTimeZone;
use Dom\HTMLDocument;
use Dom\HTMLElement;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;
use stdClass;

use function Flow\Types\DSL\type_from_array;
use function Flow\Types\DSL\type_html_element;
use function Flow\Types\DSL\type_string;
use function preg_replace;

#[RequiresPhp('>= 8.4.0')]
final class HTMLElementTypeTest extends TestCase
{
    public static function assert_data_provider(): Generator
    {
        yield 'valid HTMLElement' => [
            // @mago-expect analysis:unavailable-method
            'value' => HTMLDocument::createFromString(
                '<!DOCTYPE html><html><head></head><body></body></html>',
            )->querySelector('body'),
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

        yield 'incomplete HTML' => [
            'value' => '<div><span>1</span></div>',
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'random object' => [
            'value' => new stdClass(),
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider(): Generator
    {
        yield 'valid HTMLElement' => [
            // @mago-expect analysis:unavailable-method
            'value' => HTMLDocument::createFromString(
                '<!DOCTYPE html><html lang="en"><head></head><body><div><span>1</span></div></body></html>',
            )->querySelector('body'),
            'expected' => '<body><div><span>1</span></div></body>',
            'exceptionClass' => null,
        ];

        yield 'valid HTML string' => [
            'value' => '<!DOCTYPE html><html lang="en"><head></head><body><div><span>1</span></div></body></html>',
            'expected' => '<html lang="en"><head></head><body><div><span>1</span></div></body></html>',
            'exceptionClass' => null,
        ];

        yield 'valid HTML with spaces' => [
            'value' => '<!DOCTYPE html><html>   <head><title></title></head>    <body><p>invalid</p>  </body>  </html>',
            'expected' => '<html>   <head><title></title></head>    <body><p>invalid</p>  </body>  </html>',
            'exceptionClass' => null,
        ];

        yield 'valid HTML with new lines' => [
            'value' => <<<'HTML'
                <!DOCTYPE html>
                <html>
                    <head><title></title></head>
                    <body>
                        <p> invalid</p>
                    </body>
                </html>
                HTML,
            'expected' => '<html>    <head><title></title></head>    <body>        <p> invalid</p>    </body></html>',
            'exceptionClass' => null,
        ];

        yield 'missing doctype' => [
            'value' => '<html><body><div><span>bar</span></div></body></html>',
            'expected' => '<html><body><div><span>bar</span></div></body></html>',
            'exceptionClass' => null,
        ];

        yield 'missing head' => [
            'value' => '<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.0 Transitional//EN" "http://www.w3.org/TR/REC-html40/loose.dtd">
<html><body><p>invalid</p></body></html>',
            'expected' => '<html><body><p>invalid</p></body></html>',
            'exceptionClass' => null,
        ];

        yield 'random object' => [
            'value' => new stdClass(),
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];
    }

    public static function is_valid_data_provider(): Generator
    {
        yield 'valid HTMLDocument' => [
            // @mago-expect analysis:unavailable-method
            'value' => HTMLDocument::createFromString(
                '<!DOCTYPE html><html lang="en"><head></head><body><div><span>1</span></div></body></html>',
            )->querySelector('body'),
            'expected' => true,
        ];

        yield 'valid HTML string' => [
            'value' => '<!DOCTYPE html><html lang="en"><head></head><body><div><span>1</span></div></body></html>',
            'expected' => false,
        ];

        yield 'invalid HTML string' => [
            'value' => '<html lang="en"><head></head><body><div><span>1</span></div></body></html>',
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
            type_html_element()->assert($value);
        } else {
            static::assertInstanceOf(HTMLElement::class, type_html_element()->assert($value));
        }
    }

    /**
     * @param null|class-string<\Throwable> $exceptionClass
     */
    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, ?string $expected = null, ?string $exceptionClass = null): void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_html_element()->cast($value);

            return;
        }

        static::assertNotNull($expected);

        $result = type_html_element()->assert(type_html_element()->cast($value));
        $canonical = type_string()->assert($result->C14N());
        self::assertHtmlEquals($expected, $canonical);
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected): void
    {
        static::assertSame($expected, type_html_element()->isValid($value));
    }

    public function test_normalization(): void
    {
        $type = type_html_element();
        $normalized = $type->normalize();

        static::assertEquals($type, type_from_array($normalized));
    }

    public function test_to_string(): void
    {
        static::assertSame('html_element', type_html_element()->toString());
    }

    private function assertHtmlEquals(string $expected, string $html): void
    {
        self::assertEquals(preg_replace('/\s*/', '', $expected), preg_replace('/\s*/', '', $html));
    }
}
