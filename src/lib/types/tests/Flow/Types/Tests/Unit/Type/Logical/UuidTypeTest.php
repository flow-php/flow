<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\type_uuid;
use Flow\ETL\Exception\{CastingException, InvalidTypeException};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Ramsey\Uuid\Uuid;

final class UuidTypeTest extends TestCase
{
    public static function invalid_assert_data_provider() : \Generator
    {
        yield ['string'];
        yield ['49e952c8-80ec-4910-a1d6-a19bd46b163d'];
        yield [false];
        yield [124.25];
        yield [[1, 2]];
        yield [new \stdClass()];
        yield [new \DateTimeImmutable()];
        yield [new \DateTime()];
        yield [new \DateTimeZone('UTC')];
    }

    public static function successful_assert_data_provider() : \Generator
    {
        yield [new \Flow\Types\Value\Uuid('49e952c8-80ec-4910-a1d6-a19bd46b163d')];
    }

    public function test_casting_integer_to_uuid() : void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage('Can\'t cast "int" into "uuid" type');

        type_uuid()->cast(1);
    }

    public function test_casting_ramsey_uuid_to_uuid() : void
    {
        self::assertEquals(
            new \Flow\Types\Value\Uuid('6c2f6e0e-8d8e-4e9e-8f0e-5a2d9c1c4f6e'),
            type_uuid()->cast(Uuid::fromString('6c2f6e0e-8d8e-4e9e-8f0e-5a2d9c1c4f6e'))
        );
    }

    public function test_casting_string_to_uuid() : void
    {
        self::assertEquals(
            new \Flow\Types\Value\Uuid('6c2f6e0e-8d8e-4e9e-8f0e-5a2d9c1c4f6e'),
            type_uuid()->cast('6c2f6e0e-8d8e-4e9e-8f0e-5a2d9c1c4f6e')
        );
    }

    public function test_casting_xml_element_to_uuid() : void
    {
        $uuid = Uuid::fromString('6c2f6e0e-8d8e-4e9e-8f0e-5a2d9c1c4f6e')->toString();

        self::assertEquals(
            $uuid,
            type_uuid()->cast(new \DOMElement('element', $uuid))
        );
    }

    #[DataProvider('invalid_assert_data_provider')]
    public function test_invalid_assert(mixed $value) : void
    {
        $this->expectException(InvalidTypeException::class);
        type_uuid()->assert($value);
    }

    public function test_is_valid() : void
    {
        self::assertFalse(type_uuid()->isValid('f6d6e0e8-4b7e-4b0e-8d7a-ff0a0c9c9a5a'));
        self::assertFalse(type_uuid()->isValid('f6d6e0e8-4b7e-4b0e-8d7a-ff0a0c9c9a5'));
        self::assertFalse(type_uuid()->isValid('2'));
        self::assertFalse(type_uuid()->isValid(Uuid::uuid4()));
        self::assertFalse(type_uuid()->isValid(\Symfony\Component\Uid\Uuid::v4()));
        self::assertTrue(type_uuid()->isValid(new \Flow\Types\Value\Uuid(Uuid::uuid4())));
    }

    #[DataProvider('successful_assert_data_provider')]
    public function test_successful_assert(mixed $value) : void
    {
        self::assertInstanceOf(\Flow\Types\Value\Uuid::class, type_uuid()->assert($value));
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'uuid',
            type_uuid()->toString()
        );
    }
}
