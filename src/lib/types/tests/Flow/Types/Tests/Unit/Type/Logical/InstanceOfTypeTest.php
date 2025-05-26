<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\{type_from_array, type_instance_of};
use Flow\Types\Exception\InvalidTypeException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class InstanceOfTypeTest extends TestCase
{
    public static function assert_data_provider() : \Generator
    {
        yield 'valid DateTimeImmutable for DateTimeImmutable class' => [
            'value' => new \DateTimeImmutable(),
            'class' => \DateTimeImmutable::class,
            'exceptionClass' => null,
        ];

        yield 'valid DateTimeImmutable for DateTimeInterface class' => [
            'value' => new \DateTimeImmutable(),
            'class' => \DateTimeInterface::class,
            'exceptionClass' => null,
        ];

        yield 'valid DateTime for DateTimeInterface class' => [
            'value' => new \DateTime(),
            'class' => \DateTimeInterface::class,
            'exceptionClass' => null,
        ];

        yield 'invalid string' => [
            'value' => 'string',
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

        yield 'invalid stdClass' => [
            'value' => new \stdClass(),
            'class' => \DateTimeImmutable::class,
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider() : \Generator
    {
        yield 'stdClass object' => [
            'value' => (object) ['foo' => 'bar'],
            'class' => \stdClass::class,
            'expected' => (object) ['foo' => 'bar'],
            'exceptionClass' => null,
        ];
    }

    public static function is_valid_data_provider() : \Generator
    {
        yield 'valid stdClass for stdClass class' => [
            'value' => new \stdClass(),
            'class' => \stdClass::class,
            'expected' => true,
        ];

        yield 'invalid null' => [
            'value' => null,
            'class' => \stdClass::class,
            'expected' => false,
        ];

        yield 'invalid string' => [
            'value' => 'one',
            'class' => \stdClass::class,
            'expected' => false,
        ];

        yield 'invalid ArrayIterator' => [
            'value' => new \ArrayIterator([]),
            'class' => \stdClass::class,
            'expected' => false,
        ];
    }

    /**
     * @param class-string $class
     */
    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, string $class, ?string $exceptionClass = null) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_instance_of($class)->assert($value);
        } else {
            self::assertInstanceOf($class, type_instance_of($class)->assert($value));
        }
    }

    /**
     * @param class-string $class
     */
    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, string $class, mixed $expected, ?string $exceptionClass) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_instance_of($class)->cast($value);
        } else {
            $result = type_instance_of($class)->cast($value);
            self::assertEquals($expected, $result);
            self::assertInstanceOf($class, $result);
        }
    }

    /**
     * @param class-string $class
     */
    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, string $class, bool $expected) : void
    {
        self::assertSame($expected, type_instance_of($class)->isValid($value));
    }

    public function test_normalization() : void
    {
        $type = type_instance_of(\DateTimeImmutable::class);
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'object<DateTimeImmutable>',
            type_instance_of(\DateTimeImmutable::class)->toString()
        );
    }
}
