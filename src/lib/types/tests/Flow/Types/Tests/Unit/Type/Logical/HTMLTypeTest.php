<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\{type_from_array, type_html};
use Flow\Types\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Value\HTMLDocument;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class HTMLTypeTest extends TestCase
{
    public static function assert_data_provider() : \Generator
    {
        yield 'valid HTMLDocument' => [
            'value' => new HTMLDocument(''),
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
        if (PHP_VERSION_ID >= 80400) {
            yield 'string to HTML' => [
                'value' => '<!DOCTYPE html><html lang="en"><body><div><span>1</span></div></body></html>',
                'expected' => '<!DOCTYPE html><html lang="en"><body><div><span>1</span></div></body></html>',
                'exceptionClass' => null,
            ];
        } else {
            yield 'string to HTML' => [
                'value' => '<!DOCTYPE html><html lang="en"><body><div><span>1</span></div></body></html>',
                'expected' => <<<'HTML'
<!DOCTYPE html>
<html lang="en"><body><div><span>1</span></div></body></html>
HTML,
                'exceptionClass' => null,
            ];
        }

        yield 'incomplete string to HTML' => [
            'value' => '<div><span>1</span></div>',
            'expected' => <<<'HTML'
<div><span>1</span></div>
HTML,
            'exceptionClass' => null,
        ];

        yield 'object to HTML' => [
            'value' => new \stdClass(),
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];
    }

    public static function is_valid_data_provider() : \Generator
    {
        yield 'valid HTMLDocument' => [
            'value' => new HTMLDocument(''),
            'expected' => true,
        ];

        yield 'invalid HTML string' => [
            'value' => '<html></html>',
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
            type_html()->assert($value);
        } else {
            self::assertInstanceOf(HTMLDocument::class, type_html()->assert($value));
        }
    }

    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, mixed $expected, ?string $exceptionClass) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_html()->cast($value);
        } else {
            $result = type_html()->cast($value);
            self::assertSame($expected, $result->toString());
        }
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
}
