<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\type_positive_integer;
use Flow\Types\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type\TypeFactory;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class PositiveIntegerTypeTest extends TestCase
{
    public static function assertion_data_provider() : \Generator
    {
        yield [1, false];
        yield [0, true];
        yield [-1, true];
        yield [PHP_INT_MAX, false];
    }

    public static function casting_data_provider() : \Generator
    {
        yield [1, 1, false];
        yield [0, 0, true];
        yield [-1, -1, true];
        yield [PHP_INT_MAX, PHP_INT_MAX, false];
    }

    public static function validation_data_provider() : \Generator
    {
        yield [1, true];
        yield [0, false];
        yield [-1, false];
        yield [PHP_INT_MAX, true];
    }

    #[DataProvider('assertion_data_provider')]
    public function test_assert(mixed $value, bool $exception) : void
    {
        if ($exception) {
            $this->expectException(InvalidTypeException::class);
        }

        self::assertSame($value, type_positive_integer()->assert($value));
    }

    #[DataProvider('casting_data_provider')]
    public function test_cast(mixed $value, mixed $output, bool $exception) : void
    {
        if ($exception) {
            $this->expectException(CastingException::class);
        }

        self::assertEquals($output, type_positive_integer()->cast($value));
    }

    #[DataProvider('validation_data_provider')]
    public function test_is_valid(mixed $value, bool $result) : void
    {
        self::assertEquals($result, type_positive_integer()->isValid($value));
    }

    public function test_normalization() : void
    {
        $type = type_positive_integer();

        self::assertSame(
            [
                'type' => 'positive_integer',
            ],
            $type->normalize()
        );

        self::assertEquals(
            type_positive_integer(),
            TypeFactory::fromArray($type->normalize())
        );
    }
}
