<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\type_instance_of;
use Flow\Types\Exception\InvalidTypeException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class InstanceOfTypeTest extends TestCase
{
    public static function invalid_assert_data_provider() : \Generator
    {
        yield ['string', \DateTimeImmutable::class];
        yield [false, \DateTimeImmutable::class];
        yield [124.25, \DateTimeImmutable::class];
        yield [[1, 2], \DateTimeImmutable::class];
        yield [new \stdClass(), \DateTimeImmutable::class];
    }

    public static function successful_assert_data_provider() : \Generator
    {
        yield [new \DateTimeImmutable(), \DateTimeImmutable::class];
        yield [new \DateTimeImmutable(), \DateTimeInterface::class];
        yield [new \DateTime(), \DateTimeInterface::class];
    }

    public function test_casting_string_to_object() : void
    {
        self::assertEquals(
            (object) ['foo' => 'bar'],
            type_instance_of(\stdClass::class)->cast((object) ['foo' => 'bar'])
        );
        self::assertInstanceOf(
            \stdClass::class,
            type_instance_of(\stdClass::class)->cast((object) ['foo' => 'bar'])
        );
    }

    /**
     * @param class-string $class
     */
    #[DataProvider('invalid_assert_data_provider')]
    public function test_invalid_assert(mixed $value, string $class) : void
    {
        $this->expectException(InvalidTypeException::class);
        type_instance_of($class)->assert($value);
    }

    /**
     * @param class-string $class
     */
    #[DataProvider('successful_assert_data_provider')]
    public function test_successful_assert(mixed $value, string $class) : void
    {
        self::assertInstanceOf($class, (type_instance_of($class))->assert($value));
    }

    public function test_valid() : void
    {
        /** @phpstan-ignore-next-line  */
        self::assertFalse(type_instance_of(\stdClass::class)->isValid(null));
        /** @phpstan-ignore-next-line  */
        self::assertFalse(type_instance_of(\stdClass::class)->isValid('one'));
        /** @phpstan-ignore-next-line  */
        self::assertFalse(type_instance_of(\stdClass::class)->isValid(new \ArrayIterator([])));
        /** @phpstan-ignore-next-line  */
        self::assertTrue(type_instance_of(\stdClass::class)->isValid(new \stdClass()));
    }
}
