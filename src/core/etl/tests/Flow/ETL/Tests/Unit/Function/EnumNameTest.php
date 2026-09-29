<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;
use UnitEnum;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\enum_name;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_string;

final class EnumNameTest extends FlowTestCase
{
    public function test_enum_name_accepts_literal_enum(): void
    {
        static::assertSame('one', (new FunctionContext(flow_context()))->eval(
            enum_name(BackedStringEnum::one),
            [],
            schema(),
        ));
    }

    public function test_enum_name_carries_string_type(): void
    {
        static::assertTrue(type_equals(type_string(), enum_name(ref('e'))->returns()));
    }

    public function test_enum_name_from_scalar_function_chain(): void
    {
        static::assertSame('one', (new FunctionContext(flow_context()))->eval(
            ref('e')->enumName(),
            [
                'e' => BackedIntEnum::one,
            ],
            schema(enum_schema('e', BackedIntEnum::class)),
        ));
    }

    #[TestWith([BackedStringEnum::one])]
    #[TestWith([BackedIntEnum::one])]
    #[TestWith([BasicEnum::one])]
    public function test_enum_name_returns_case_name(UnitEnum $enum): void
    {
        static::assertSame('one', (new FunctionContext(flow_context()))->eval(
            enum_name(ref('e')),
            ['e' => $enum],
            schema(enum_schema('e', $enum::class)),
        ));
    }

    #[TestWith([null])]
    #[TestWith(['foo'])]
    #[TestWith([42])]
    public function test_enum_name_throws_in_strict_mode(mixed $input): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('EnumName function requires a UnitEnum value');

        $context = flow_context(config());
        (new FunctionContext($context))->eval(enum_name($input), [], schema());
    }
}
