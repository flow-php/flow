<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use function Flow\Types\DSL\type_object;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type\TypeFactory;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ObjectTypeTest extends TestCase
{
    public static function cast_data_provider() : \Generator
    {
        yield ['string', (object) 'string'];
        yield [['foo' => 'bar'], json_decode('{"foo":"bar"}')];
    }

    public static function invalid_assert_data_provider() : \Generator
    {
        yield ['string'];
        yield [false];
        yield [124.25];
        yield [[1, 2]];
    }

    public static function successful_assert_data_provider() : \Generator
    {
        yield [new \DateTimeImmutable()];
        yield [new \DateTimeImmutable()];
        yield [new \DateTime()];
        yield [new \stdClass()];
    }

    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, object $expected) : void
    {
        self::assertEquals($expected, type_object()->cast($value));
    }

    /**
     * @param class-string $class
     */
    #[DataProvider('invalid_assert_data_provider')]
    public function test_invalid_assert(mixed $value) : void
    {
        $this->expectException(InvalidTypeException::class);
        type_object()->assert($value);
    }

    public function test_is_valid() : void
    {
        self::assertTrue(type_object()->isValid(new \stdClass()));
        self::assertTrue(type_object()->isValid(new \DateTimeImmutable()));
        self::assertTrue(type_object()->isValid(new \DateTime()));
        self::assertFalse(type_object()->isValid('string'));
        self::assertFalse(type_object()->isValid(false));
        self::assertFalse(type_object()->isValid(124.25));
    }

    public function test_normalization() : void
    {
        $type = type_object();

        self::assertSame(
            [
                'type' => 'object',
            ],
            $type->normalize()
        );

        self::assertSame('object', $type->toString());
        self::assertEquals(type_object(), TypeFactory::fromArray($type->normalize()));
    }

    /**
     * @param class-string $class
     */
    #[DataProvider('successful_assert_data_provider')]
    public function test_successful_assert(object $value) : void
    {
        self::assertInstanceOf($value::class, type_object()->assert($value));
    }

    public function test_to_string() : void
    {
        $type = type_object();

        self::assertSame('object', $type->toString());
    }
}
