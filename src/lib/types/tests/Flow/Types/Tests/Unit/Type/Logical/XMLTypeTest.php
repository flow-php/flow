<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\type_xml;
use Flow\Types\Exception\{CastingException};
use Flow\Types\Exception\InvalidTypeException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class XMLTypeTest extends TestCase
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
        yield [new \DateTimeImmutable()];
    }

    public static function successful_assert_data_provider() : \Generator
    {
        yield [new \DOMDocument()];
    }

    public function test_casting_integer_to_xml() : void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage('Can\'t cast "int" into "xml" type');

        type_xml()->cast(1);
    }

    public function test_casting_string_to_xml() : void
    {
        self::assertSame(
            '<?xml version="1.0"?>' . "\n" . '<items><item>1</item></items>' . "\n",
            type_xml()->cast('<items><item>1</item></items>')->saveXML()
        );
    }

    #[DataProvider('invalid_assert_data_provider')]
    public function test_invalid_assert(mixed $value) : void
    {
        $this->expectException(InvalidTypeException::class);
        type_xml()->assert($value);
    }

    public function test_is_valid() : void
    {
        self::assertTrue(type_xml()->isValid(new \DOMDocument()));
        self::assertFalse(type_xml()->isValid('<xml></xml>'));
        self::assertFalse(type_xml()->isValid('2020-01-01'));
        self::assertFalse(type_xml()->isValid('2020-01-01 00:00:00'));
    }

    #[DataProvider('successful_assert_data_provider')]
    public function test_successful_assert(mixed $value) : void
    {
        self::assertInstanceOf(\DOMDocument::class, type_xml()->assert($value));
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'xml',
            type_xml()->toString()
        );
    }
}
