<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\{type_class_string, type_from_array};
use Flow\Types\Exception\{CastingException, InvalidTypeException};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ClassStringTypeTest extends TestCase
{
    public static function assert_data_provider() : \Generator
    {
        yield 'valid DateTimeImmutable class string for DateTimeInterface class' => [
            'value' => \DateTimeImmutable::class,
            'class' => \DateTimeInterface::class,
            'exceptionClass' => null,
        ];

        yield 'valid DateTimeImmutable class string for DateTimeImmutable class' => [
            'value' => \DateTimeImmutable::class,
            'class' => \DateTimeImmutable::class,
            'exceptionClass' => null,
        ];

        yield 'valid DateTime class string for DateTimeInterface class' => [
            'value' => \DateTime::class,
            'class' => \DateTimeInterface::class,
            'exceptionClass' => null,
        ];

        yield 'valid stdClass class string for stdClass class' => [
            'value' => \stdClass::class,
            'class' => \stdClass::class,
            'exceptionClass' => null,
        ];

        yield 'valid DateTimeImmutable class string for no class constraint' => [
            'value' => \DateTimeImmutable::class,
            'class' => null,
            'exceptionClass' => null,
        ];

        yield 'valid stdClass class string for no class constraint' => [
            'value' => \stdClass::class,
            'class' => null,
            'exceptionClass' => null,
        ];

        yield 'invalid non-class string for no class constraint' => [
            'value' => 'not-a-class',
            'class' => null,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid non-class string' => [
            'value' => 'not-a-class',
            'class' => \DateTimeImmutable::class,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTime class string for DateTimeImmutable class' => [
            'value' => \DateTime::class,
            'class' => \DateTimeImmutable::class,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid stdClass class string for DateTimeImmutable class' => [
            'value' => \stdClass::class,
            'class' => \DateTimeImmutable::class,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid integer' => [
            'value' => 123,
            'class' => \DateTimeImmutable::class,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid boolean' => [
            'value' => false,
            'class' => \DateTimeImmutable::class,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid float' => [
            'value' => 124.25,
            'class' => \DateTimeImmutable::class,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid array' => [
            'value' => [1, 2],
            'class' => \DateTimeImmutable::class,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid object' => [
            'value' => new \stdClass(),
            'class' => \DateTimeImmutable::class,
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider() : \Generator
    {
        yield 'valid stdClass class string' => [
            'value' => \stdClass::class,
            'class' => \stdClass::class,
            'expected' => \stdClass::class,
            'exceptionClass' => null,
        ];

        yield 'valid DateTimeImmutable class string for DateTimeInterface' => [
            'value' => \DateTimeImmutable::class,
            'class' => \DateTimeInterface::class,
            'expected' => \DateTimeImmutable::class,
            'exceptionClass' => null,
        ];

        yield 'cast DateTimeImmutable object to class string' => [
            'value' => new \DateTimeImmutable(),
            'class' => \DateTimeInterface::class,
            'expected' => \DateTimeImmutable::class,
            'exceptionClass' => null,
        ];

        yield 'cast DateTime object to class string' => [
            'value' => new \DateTime(),
            'class' => \DateTimeInterface::class,
            'expected' => \DateTime::class,
            'exceptionClass' => null,
        ];

        yield 'cast stdClass object to class string' => [
            'value' => new \stdClass(),
            'class' => \stdClass::class,
            'expected' => \stdClass::class,
            'exceptionClass' => null,
        ];

        yield 'cast DateTimeImmutable object to class string for no class constraint' => [
            'value' => new \DateTimeImmutable(),
            'class' => null,
            'expected' => \DateTimeImmutable::class,
            'exceptionClass' => null,
        ];

        yield 'cast stdClass object to class string for no class constraint' => [
            'value' => new \stdClass(),
            'class' => null,
            'expected' => \stdClass::class,
            'exceptionClass' => null,
        ];

        yield 'valid class string for no class constraint' => [
            'value' => \ArrayIterator::class,
            'class' => null,
            'expected' => \ArrayIterator::class,
            'exceptionClass' => null,
        ];

        yield 'invalid non-class string' => [
            'value' => 'not-a-class',
            'class' => \stdClass::class,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'invalid incompatible class string' => [
            'value' => \stdClass::class,
            'class' => \DateTimeInterface::class,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'invalid incompatible object' => [
            'value' => new \stdClass(),
            'class' => \DateTimeInterface::class,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'invalid integer' => [
            'value' => 123,
            'class' => \stdClass::class,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'invalid array' => [
            'value' => [1, 2],
            'class' => \stdClass::class,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];
    }

    public static function is_valid_data_provider() : \Generator
    {
        yield 'valid stdClass class string for stdClass class' => [
            'value' => \stdClass::class,
            'class' => \stdClass::class,
            'expected' => true,
        ];

        yield 'valid DateTimeImmutable class string for DateTimeInterface class' => [
            'value' => \DateTimeImmutable::class,
            'class' => \DateTimeInterface::class,
            'expected' => true,
        ];

        yield 'valid DateTime class string for DateTimeInterface class' => [
            'value' => \DateTime::class,
            'class' => \DateTimeInterface::class,
            'expected' => true,
        ];

        yield 'valid DateTimeImmutable class string for no class constraint' => [
            'value' => \DateTimeImmutable::class,
            'class' => null,
            'expected' => true,
        ];

        yield 'valid stdClass class string for no class constraint' => [
            'value' => \stdClass::class,
            'class' => null,
            'expected' => true,
        ];

        yield 'valid ArrayIterator class string for no class constraint' => [
            'value' => \ArrayIterator::class,
            'class' => null,
            'expected' => true,
        ];

        yield 'invalid non-class string for no class constraint' => [
            'value' => 'not-a-class',
            'class' => null,
            'expected' => false,
        ];

        yield 'invalid non-class string' => [
            'value' => 'not-a-class',
            'class' => \stdClass::class,
            'expected' => false,
        ];

        yield 'invalid DateTime class string for DateTimeImmutable class' => [
            'value' => \DateTime::class,
            'class' => \DateTimeImmutable::class,
            'expected' => false,
        ];

        yield 'invalid stdClass class string for DateTimeImmutable class' => [
            'value' => \stdClass::class,
            'class' => \DateTimeImmutable::class,
            'expected' => false,
        ];

        yield 'invalid null' => [
            'value' => null,
            'class' => \stdClass::class,
            'expected' => false,
        ];

        yield 'invalid integer' => [
            'value' => 123,
            'class' => \stdClass::class,
            'expected' => false,
        ];

        yield 'invalid boolean' => [
            'value' => false,
            'class' => \stdClass::class,
            'expected' => false,
        ];

        yield 'invalid float' => [
            'value' => 124.25,
            'class' => \stdClass::class,
            'expected' => false,
        ];

        yield 'invalid array' => [
            'value' => [1, 2],
            'class' => \stdClass::class,
            'expected' => false,
        ];

        yield 'invalid object' => [
            'value' => new \stdClass(),
            'class' => \stdClass::class,
            'expected' => false,
        ];
    }

    /**
     * @param null|class-string $class
     */
    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, ?string $class, ?string $exceptionClass = null) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_class_string($class)->assert($value);
        } else {
            $result = type_class_string($class)->assert($value);
            self::assertIsString($result);
            self::assertTrue(\class_exists($result) || \interface_exists($result));

            if ($class !== null) {
                self::assertTrue(\is_a($result, $class, true));
            }
        }
    }

    /**
     * @param null|class-string $class
     */
    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, ?string $class, mixed $expected, ?string $exceptionClass) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_class_string($class)->cast($value);
        } else {
            $result = type_class_string($class)->cast($value);
            self::assertSame($expected, $result);
            self::assertIsString($result);
            self::assertTrue(\class_exists($result) || \interface_exists($result));

            if ($class !== null) {
                self::assertTrue(\is_a($result, $class, true));
            }
        }
    }

    /**
     * @param null|class-string $class
     */
    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, ?string $class, bool $expected) : void
    {
        self::assertSame($expected, type_class_string($class)->isValid($value));
    }

    public function test_normalization() : void
    {
        $type = type_class_string(\DateTimeImmutable::class);
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
    }

    public function test_normalization_without_class() : void
    {
        $type = type_class_string();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'class-string<DateTimeImmutable>',
            type_class_string(\DateTimeImmutable::class)->toString()
        );

        self::assertSame(
            'class-string<DateTimeInterface>',
            type_class_string(\DateTimeInterface::class)->toString()
        );

        self::assertSame(
            'class-string',
            type_class_string()->toString()
        );
    }
}
