<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use function Flow\Types\DSL\type_string;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Tests\Unit\Type\Fixtures\StringableObject;
use PHPUnit\Framework\Attributes\{DataProvider, TestWith};
use PHPUnit\Framework\TestCase;

final class StringTypeTest extends TestCase
{
    public static function invalid_assert_data_provider() : \Generator
    {
        yield [null, false];
        yield [false];
        yield [124.25];
        yield [[1, 2]];
        yield [new \stdClass()];
        yield [new \DateTimeImmutable()];
        yield [new \DateTime()];
        yield [new \DateTimeZone('UTC')];
    }

    public static function string_castable_data_provider() : \Generator
    {
        yield 'string' => ['string', 'string'];
        yield 'int' => [1, '1'];
        yield 'float' => [1.1, '1.1'];
        yield 'bool' => [true, 'true'];
        yield 'array' => [[1, 2, 3], '[1,2,3]'];
        yield 'DateTimeInterface' => [new \DateTimeImmutable('2021-01-01 00:00:00'), '2021-01-01T00:00:00+00:00'];
        yield 'Stringable' => [new class() implements \Stringable {
            public function __toString() : string
            {
                return 'stringable';
            }
        }, 'stringable'];
        yield 'DOMDocument' => [new \DOMDocument(), '<?xml version="1.0"?>'];

        $xml = (new \DOMDocument());
        $xml->loadXML('<xml>Some Happy XML</xml>');

        yield 'Not Empty DOMDocument' => [$xml, '<xml>Some Happy XML</xml>'];
        yield 'DOMElement' => [new \DOMElement('element'), '<element/>'];
        yield 'DateTimeZone' => [new \DateTimeZone('UTC'), 'UTC'];
    }

    public static function successful_assert_data_provider() : \Generator
    {
        yield ['1234'];
        yield ['abcd'];
    }

    #[DataProvider('string_castable_data_provider')]
    public function test_casting_different_data_types_to_string(mixed $value, string $expected) : void
    {
        self::assertSame($expected, \trim((string) type_string()->cast($value)));
    }

    #[DataProvider('invalid_assert_data_provider')]
    public function test_invalid_assert(mixed $value) : void
    {
        $this->expectException(InvalidTypeException::class);
        type_string()->assert($value);
    }

    #[TestWith([new \DateTimeImmutable(), false])]
    #[TestWith([new \DateInterval('P1D'), false])]
    #[TestWith([new StringableObject(), true])]
    public function test_is_stringable(mixed $value, bool $stringable) : void
    {
        if ($stringable) {
            self::assertTrue(type_string()->isStringable($value));
        } else {
            self::assertFalse(type_string()->isStringable($value));
        }
    }

    #[DataProvider('successful_assert_data_provider')]
    public function test_successful_assert(mixed $value) : void
    {
        /** @phpstan-ignore-next-line */
        self::assertIsString(type_string()->assert($value));
    }
}
