<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\type_xml_element;
use Flow\ETL\Exception\InvalidTypeException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class XMLElementTypeTest extends TestCase
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
        yield [new \DOMElement('xml')];
    }

    #[DataProvider('invalid_assert_data_provider')]
    public function test_invalid_assert(mixed $value) : void
    {
        $this->expectException(InvalidTypeException::class);
        type_xml_element()->assert($value);
    }

    public function test_is_valid() : void
    {
        self::assertTrue(type_xml_element()->isValid(new \DOMElement('xml')));
        self::assertFalse(type_xml_element()->isValid('<xml></xml>'));
        self::assertFalse(type_xml_element()->isValid('2020-01-01'));
        self::assertFalse(type_xml_element()->isValid('2020-01-01 00:00:00'));
    }

    #[DataProvider('successful_assert_data_provider')]
    public function test_successful_assert(mixed $value) : void
    {
        self::assertInstanceOf(\DOMElement::class, type_xml_element()->assert($value));
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'xml_element',
            type_xml_element()->toString()
        );
    }
}
