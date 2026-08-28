<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;
use UnitEnum;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\enum_entry;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\enum_value;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;

final class EnumValueTest extends FlowTestCase
{
    public function test_enum_value_accepts_literal_enum(): void
    {
        static::assertSame(1, enum_value(BackedIntEnum::one)->eval(row(), flow_context()));
    }

    public function test_enum_value_from_scalar_function_chain(): void
    {
        static::assertSame(1, ref('e')->enumValue()->eval(row(enum_entry('e', BackedIntEnum::one)), flow_context()));
    }

    #[TestWith([BackedStringEnum::one, 'one'])]
    #[TestWith([BackedIntEnum::one, 1])]
    public function test_enum_value_returns_backing_value(UnitEnum $enum, int|string $expected): void
    {
        static::assertSame($expected, enum_value(ref('e'))->eval(row(enum_entry('e', $enum)), flow_context()));
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
        enum_value($input)->eval(row(), $context);
    }

    public function test_a_non_enum_operand_is_refused_at_bind(): void
    {
        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve(enum_value(ref('e')), schema(str_schema('e')));

        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage('which is not an enum');

        $resolved->returns();
    }

    public function test_enum_value_type_is_derived_from_int_backing_value(): void
    {
        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve(
            enum_value(ref('e')),
            schema(enum_schema('e', BackedIntEnum::class)),
        );

        static::assertTrue(type_equals(type_integer(), $resolved->returns()));
    }

    public function test_enum_value_type_is_derived_from_string_backing_value(): void
    {
        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve(
            enum_value(ref('e')),
            schema(enum_schema('e', BackedStringEnum::class)),
        );

        static::assertTrue(type_equals(type_string(), $resolved->returns()));
    }
}
