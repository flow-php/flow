<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use function Flow\Types\DSL\type_null;
use Flow\ETL\Exception\InvalidTypeException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class NullTypeTest extends TestCase
{
    public static function invalid_assert_data_provider() : \Generator
    {
        yield ['string'];
        yield [false];
        yield [124.25];
        yield [124];
        yield [[1, 2]];
        yield [new \stdClass()];
        yield [new \DateTimeImmutable()];
        yield [new \DateTime()];
        yield [new \DateTimeZone('UTC')];
    }

    #[DataProvider('invalid_assert_data_provider')]
    public function test_invalid_assert(mixed $value) : void
    {
        $this->expectException(InvalidTypeException::class);
        (type_null())->assert($value);
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'null',
            type_null()->toString()
        );
    }

    public function test_valid() : void
    {
        self::assertTrue(
            type_null()->isValid(null)
        );
        self::assertFalse(
            type_null()->isValid('one')
        );
        self::assertFalse(
            type_null()->isValid([1, 2])
        );
        self::assertFalse(
            type_null()->isValid(123)
        );
    }
}
