<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Native;

use function Flow\ETL\DSL\{type_object};
use Flow\ETL\Exception\InvalidTypeException;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class ObjectTypeTest extends FlowTestCase
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
            type_object(\stdClass::class)->cast((object) ['foo' => 'bar'])
        );
        self::assertInstanceOf(
            \stdClass::class,
            type_object(\stdClass::class)->cast((object) ['foo' => 'bar'])
        );
    }

    #[DataProvider('invalid_assert_data_provider')]
    public function test_invalid_assert(mixed $value, string $class) : void
    {
        $this->expectException(InvalidTypeException::class);
        type_object($class)->assert($value);
    }

    #[DataProvider('successful_assert_data_provider')]
    public function test_successful_assert(mixed $value, string $class) : void
    {
        self::assertInstanceOf($class, (type_object($class))->assert($value));
    }

    public function test_valid() : void
    {
        self::assertTrue(
            type_object(\stdClass::class, true)->isValid(null)
        );
        self::assertFalse(
            type_object(\stdClass::class)->isValid(null)
        );
        self::assertFalse(
            type_object(\stdClass::class)->isValid('one')
        );
        self::assertFalse(
            type_object(\stdClass::class)->isValid(new \ArrayIterator([]))
        );
        self::assertTrue(
            type_object(\stdClass::class)->isValid(new \stdClass())
        );
    }
}
