<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\type_non_empty_string;
use Flow\Types\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type\TypeFactory;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class NonEmptyStringTypeTest extends TestCase
{
    public static function assertion_data_provider() : \Generator
    {
        yield ['string', false];
        yield ['', true];
        yield [0, true];
    }

    public static function casting_data_provider() : \Generator
    {
        yield ['string', 'string', false];
        yield ['', '', true];
        yield [0, '0', false];
        yield [1, '1', false];
        yield [new \DateTimeImmutable('2024-12-01'), '2024-12-01T00:00:00+00:00', false];
        yield [new \DateTime('2024-12-01'), '2024-12-01T00:00:00+00:00', false];
        yield [new \DateTimeZone('UTC'), 'UTC', false];
        yield [new \DOMElement('element', '2024-12-01'), '<element>2024-12-01</element>', false];
    }

    public static function validation_data_provider() : \Generator
    {
        yield ['string', true];
        yield ['', false];
        yield [0, false];
        yield [1, false];
    }

    #[DataProvider('assertion_data_provider')]
    public function test_assert(mixed $value, bool $exception) : void
    {
        if ($exception) {
            $this->expectException(InvalidTypeException::class);
        }

        self::assertSame($value, type_non_empty_string()->assert($value));
    }

    #[DataProvider('casting_data_provider')]
    public function test_cast(mixed $value, mixed $output, bool $exception) : void
    {
        if ($exception) {
            $this->expectException(CastingException::class);
        }

        self::assertSame($output, type_non_empty_string()->cast($value));
    }

    #[DataProvider('validation_data_provider')]
    public function test_is_valid(mixed $value, bool $result) : void
    {
        self::assertEquals($result, type_non_empty_string()->isValid($value));
    }

    public function test_normalization() : void
    {
        $type = type_non_empty_string();

        self::assertSame(
            [
                'type' => 'non_empty_string',
            ],
            $type->normalize()
        );

        self::assertEquals(type_non_empty_string(), TypeFactory::fromArray($type->normalize()));
    }
}
