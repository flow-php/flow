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
use function Flow\ETL\DSL\enum_name;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_string;

final class EnumNameTest extends FlowTestCase
{
    public function test_enum_name_accepts_literal_enum(): void
    {
        static::assertSame('one', enum_name(BackedStringEnum::one)->eval(row(), flow_context())?->value);
    }

    public function test_enum_name_carries_string_type(): void
    {
        $result = enum_name(ref('e'))->eval(row(enum_entry('e', BackedIntEnum::one)), flow_context());

        static::assertNotNull($result);
        static::assertTrue(type_equals(type_string(), $result->type));
    }

    public function test_enum_name_from_scalar_function_chain(): void
    {
        static::assertSame(
            'one',
            ref('e')->enumName()->eval(row(enum_entry('e', BackedIntEnum::one)), flow_context())?->value,
        );
    }

    #[TestWith([BackedStringEnum::one])]
    #[TestWith([BackedIntEnum::one])]
    #[TestWith([BasicEnum::one])]
    public function test_enum_name_returns_case_name(UnitEnum $enum): void
    {
        static::assertSame('one', enum_name(ref('e'))->eval(row(enum_entry('e', $enum)), flow_context())?->value);
    }

    #[TestWith([null])]
    #[TestWith(['foo'])]
    #[TestWith([42])]
    public function test_enum_name_returns_null_in_permissive_mode(mixed $input): void
    {
        static::assertNull(enum_name($input)->eval(row(), flow_context()));
    }

    #[TestWith([null])]
    #[TestWith(['foo'])]
    #[TestWith([42])]
    public function test_enum_name_throws_in_strict_mode(mixed $input): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('EnumName function requires a UnitEnum value');

        $context = flow_context(config());
        $context->functions()->setMode(ExecutionMode::STRICT);

        enum_name($input)->eval(row(), $context);
    }
}
