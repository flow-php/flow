<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ExecutionMode;
use Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;
use UnitEnum;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\enum_entry;
use function Flow\ETL\DSL\enum_value;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;

final class EnumValueTest extends FlowTestCase
{
    public function test_enum_value_accepts_literal_enum(): void
    {
        static::assertSame(1, enum_value(BackedIntEnum::one)->eval(row(), flow_context())?->value);
    }

    public function test_enum_value_from_scalar_function_chain(): void
    {
        static::assertSame(
            1,
            ref('e')->enumValue()->eval(row(enum_entry('e', BackedIntEnum::one)), flow_context())?->value,
        );
    }

    #[TestWith([BackedStringEnum::one, 'one'])]
    #[TestWith([BackedIntEnum::one, 1])]
    public function test_enum_value_returns_backing_value(UnitEnum $enum, int|string $expected): void
    {
        static::assertSame($expected, enum_value(ref('e'))->eval(row(enum_entry('e', $enum)), flow_context())?->value);
    }

    #[TestWith([BasicEnum::one])]
    #[TestWith([null])]
    #[TestWith(['foo'])]
    #[TestWith([42])]
    public function test_enum_value_returns_null_in_permissive_mode(mixed $input): void
    {
        static::assertNull(enum_value($input)->eval(row(), flow_context()));
    }

    #[TestWith([BasicEnum::one])]
    #[TestWith([null])]
    #[TestWith(['foo'])]
    #[TestWith([42])]
    public function test_enum_value_throws_in_strict_mode(mixed $input): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('EnumValue function requires a BackedEnum value');

        $context = flow_context(config());
        $context->functions()->setMode(ExecutionMode::STRICT);

        enum_value($input)->eval(row(), $context);
    }

    public function test_enum_value_type_is_derived_from_int_backing_value(): void
    {
        $result = enum_value(ref('e'))->eval(row(enum_entry('e', BackedIntEnum::one)), flow_context());

        static::assertNotNull($result);
        static::assertTrue(type_equals(type_integer(), $result->type));
    }

    public function test_enum_value_type_is_derived_from_string_backing_value(): void
    {
        $result = enum_value(ref('e'))->eval(row(enum_entry('e', BackedStringEnum::one)), flow_context());

        static::assertNotNull($result);
        static::assertTrue(type_equals(type_string(), $result->type));
    }
}
