<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use function Flow\Types\DSL\type_resource;
use Flow\ETL\Exception\InvalidTypeException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ResourceTypeTest extends TestCase
{
    public static function invalid_assert_data_provider() : \Generator
    {
        yield [null];
        yield ['string'];
        yield [false];
        yield [123];
        yield [[1, 2]];
        yield [new \stdClass()];
        yield [new \DateTimeImmutable()];
        yield [new \DateTime()];
        yield [new \DateTimeZone('UTC')];
    }

    public static function successful_assert_data_provider() : \Generator
    {
        yield [\fopen('php://temp/max', 'r+b')];
    }

    #[DataProvider('invalid_assert_data_provider')]
    public function test_invalid_assert(mixed $value) : void
    {
        $this->expectException(InvalidTypeException::class);
        type_resource()->assert($value);
    }

    #[DataProvider('successful_assert_data_provider')]
    public function test_successful_assert(mixed $value) : void
    {
        self::assertIsResource(type_resource()->assert($value));
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'resource',
            (type_resource())->toString()
        );
    }

    public function test_valid() : void
    {
        $handle = \fopen('php://temp/max', 'r+b');
        self::assertTrue(
            (type_resource())->isValid($handle)
        );
        \fclose($handle);
        self::assertFalse(
            (type_resource())->isValid('one')
        );
        self::assertFalse(
            (type_resource())->isValid([1, 2])
        );
        self::assertFalse(
            (type_resource())->isValid(123)
        );
    }
}
