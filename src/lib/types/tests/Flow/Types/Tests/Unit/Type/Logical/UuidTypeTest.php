<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\{type_from_array, type_uuid};
use Flow\Types\Exception\{CastingException, InvalidTypeException};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Ramsey\Uuid\Uuid;

final class UuidTypeTest extends TestCase
{
    public static function assert_data_provider() : \Generator
    {
        yield 'valid Uuid' => [
            'value' => new \Flow\Types\Value\Uuid('49e952c8-80ec-4910-a1d6-a19bd46b163d'),
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
        yield 'string to uuid' => [
            'value' => '6c2f6e0e-8d8e-4e9e-8f0e-5a2d9c1c4f6e',
            'expected' => new \Flow\Types\Value\Uuid('6c2f6e0e-8d8e-4e9e-8f0e-5a2d9c1c4f6e'),
            'exceptionClass' => null,
        ];

        yield 'ramsey uuid to uuid' => [
            'value' => Uuid::fromString('6c2f6e0e-8d8e-4e9e-8f0e-5a2d9c1c4f6e'),
            'expected' => new \Flow\Types\Value\Uuid('6c2f6e0e-8d8e-4e9e-8f0e-5a2d9c1c4f6e'),
            'exceptionClass' => null,
        ];

        yield 'xml element to uuid' => [
            'value' => new \DOMElement('element', '6c2f6e0e-8d8e-4e9e-8f0e-5a2d9c1c4f6e'),
            'expected' => '6c2f6e0e-8d8e-4e9e-8f0e-5a2d9c1c4f6e',
            'exceptionClass' => null,
        ];

        yield 'integer to uuid' => [
            'value' => 1,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];
    }

    public static function is_valid_data_provider() : \Generator
    {
        yield 'valid Flow Uuid' => [
            'value' => new \Flow\Types\Value\Uuid(Uuid::uuid4()),
            'expected' => true,
        ];

        yield 'invalid uuid string' => [
            'value' => 'f6d6e0e8-4b7e-4b0e-8d7a-ff0a0c9c9a5a',
            'expected' => false,
        ];

        yield 'invalid malformed uuid string' => [
            'value' => 'f6d6e0e8-4b7e-4b0e-8d7a-ff0a0c9c9a5',
            'expected' => false,
        ];

        yield 'invalid short string' => [
            'value' => '2',
            'expected' => false,
        ];

        yield 'invalid Ramsey Uuid' => [
            'value' => Uuid::uuid4(),
            'expected' => false,
        ];

        yield 'invalid Symfony Uuid' => [
            'value' => \Symfony\Component\Uid\Uuid::v4(),
            'expected' => false,
        ];
    }

    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, ?string $exceptionClass = null) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_uuid()->assert($value);
        } else {
            self::assertInstanceOf(\Flow\Types\Value\Uuid::class, type_uuid()->assert($value));
        }
    }

    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, mixed $expected, ?string $exceptionClass) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_uuid()->cast($value);
        } else {
            self::assertEquals($expected, type_uuid()->cast($value));
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected) : void
    {
        self::assertSame($expected, type_uuid()->isValid($value));
    }

    public function test_normalization() : void
    {
        $type = type_uuid();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'uuid',
            type_uuid()->toString()
        );
    }
}
