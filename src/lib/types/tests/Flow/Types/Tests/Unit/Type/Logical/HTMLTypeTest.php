<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\{type_from_array, type_html};
use Dom\HTMLDocument;
use Flow\Types\Exception\{CastingException, InvalidTypeException};
use PHPUnit\Framework\Attributes\{DataProvider, RequiresPhp};
use PHPUnit\Framework\TestCase;

#[RequiresPhp('>= 8.4')]
final class HTMLTypeTest extends TestCase
{
    public static function assert_data_provider() : \Generator
    {
        yield 'valid HTMLDocument' => [
            'value' => HTMLDocument::createFromString('<!DOCTYPE html><html><head></head><body></body></html>'),
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

        yield 'incomplete HTML' => [
            'value' => '<div><span>1</span></div>',
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'random object' => [
            'value' => new \stdClass(),
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider() : \Generator
    {
        yield 'valid HTMLDocument' => [
            'value' => HTMLDocument::createFromString($html = '<!DOCTYPE html><html lang="en"><head></head><body><div><span>1</span></div></body></html>'),
            'expected' => $html,
            'exceptionClass' => null,
        ];

        yield 'valid HTML string' => [
            'value' => '<!DOCTYPE html><html lang="en"><head></head><body><div><span>1</span></div></body></html>',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'valid HTML with spaces' => [
            'value' => '<!DOCTYPE html><html>   <head><title></title></head>    <body><p>invalid</p>  </body>  </html>',
            'expected' => null,
            'exceptionClass' => CastingException::class,
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
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'missing doctype' => [
            'value' => '<html><body><div><span>bar</span></div></body></html>',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'missing head' => [
            'value' => '<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.0 Transitional//EN" "http://www.w3.org/TR/REC-html40/loose.dtd">
<html><body><p>invalid</p></body></html>',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'random object' => [
            'value' => new \stdClass(),
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];
    }

    public static function is_valid_data_provider() : \Generator
    {
        yield 'valid HTMLDocument' => [
            'value' => HTMLDocument::createFromString('<!DOCTYPE html><html lang="en"><head></head><body><div><span>1</span></div></body></html>'),
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

    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, ?string $exceptionClass = null) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_html()->assert($value);
        } else {
            self::assertInstanceOf(HTMLDocument::class, type_html()->assert($value));
        }
    }

    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, mixed $expected, ?string $exceptionClass = null) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
        }

        $result = type_html()->cast($value);
        self::assertHtmlEquals($expected, $result->saveHtml());
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected) : void
    {
        self::assertSame($expected, type_html()->isValid($value));
    }

    public function test_normalization() : void
    {
        $type = type_html();
        $normalized = $type->normalize();

        self::assertEquals($type, type_from_array($normalized));
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'html',
            type_html()->toString()
        );
    }

    private function assertHtmlEquals(string $expected, string $html) : void
    {
        self::assertEquals(
            \preg_replace('/\s*/', '', $expected),
            \preg_replace('/\s*/', '', $html),
        );
    }
}
