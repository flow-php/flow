<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Php;

use Flow\ETL\Column\Php\Retype;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\Types\Type;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class RetypeTest extends TestCase
{
    /**
     * @return Generator<string, array{Type<mixed>, Type<mixed>, int}>
     */
    public static function accepted(): Generator
    {
        yield 'same type' => [type_integer(), type_integer(), 0];
        yield 'optional to required without nulls' => [type_optional(type_integer()), type_integer(), 0];
        yield 'required to optional with nulls' => [type_integer(), type_optional(type_integer()), 3];
        yield 'datetime zone change' => [type_datetime('UTC'), type_datetime('Europe/Warsaw'), 0];
        yield 'same enum class' => [type_enum(BackedIntEnum::class), type_enum(BackedIntEnum::class), 0];
        yield 'same structure element names' => [
            type_structure(['a' => type_integer()]),
            type_structure(['a' => type_optional(type_integer())]),
            0,
        ];
    }

    /**
     * @return Generator<string, array{Type<mixed>, Type<mixed>, int, string}>
     */
    public static function refused(): Generator
    {
        yield 'optional to required with nulls' => [
            type_optional(type_integer()),
            type_integer(),
            2,
            '2 null values under a required type',
        ];
        yield 'integer to string' => [
            type_integer(),
            type_string(),
            0,
            'integer and string are different column kinds',
        ];
        yield 'enum class change' => [
            type_enum(BackedIntEnum::class),
            type_enum(BackedStringEnum::class),
            0,
            'are different column kinds',
        ];
        yield 'structure element rename' => [
            type_structure(['a' => type_integer()]),
            type_structure(['b' => type_integer()]),
            0,
            'are different column kinds',
        ];
        yield 'structure element reorder' => [
            type_structure(['a' => type_integer(), 'b' => type_integer()]),
            type_structure(['b' => type_integer(), 'a' => type_integer()]),
            0,
            'are different column kinds',
        ];
    }

    /**
     * @param Type<mixed> $from
     * @param Type<mixed> $to
     */
    #[DataProvider('accepted')]
    public function test_accepts(Type $from, Type $to, int $nullCount): void
    {
        (new Retype())->assert($from, $to, $nullCount);

        $this->addToAssertionCount(1);
    }

    /**
     * @param Type<mixed> $from
     * @param Type<mixed> $to
     */
    #[DataProvider('refused')]
    public function test_refuses(Type $from, Type $to, int $nullCount, string $message): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($message);

        (new Retype())->assert($from, $to, $nullCount);
    }
}
