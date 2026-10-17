<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeImmutable;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\least;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_numeric_string;

final class LeastTest extends FlowTestCase
{
    public function test_a_non_nullable_operand_makes_the_result_not_nullable(): void
    {
        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve(
            least(ref('int'), lit(10)),
            schema(int_schema('int', nullable: true)),
        );

        static::assertSame('integer', $resolved->returns()->toString());
    }

    public function test_least_with_non_comparable_values(): void
    {
        $this->expectExceptionMessage('Cannot combine types');

        least(null, 20, lit(new DateTimeImmutable('now')))->returns();
    }

    public function test_an_integer_and_a_string_column_have_no_common_type(): void
    {
        $this->expectExceptionMessage('Cannot combine types "integer", "string"');

        least(5, 'apple')->returns();
    }

    public function test_null_only_when_every_argument_is_null(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            least(null, null),
            [
                'int' => 4,
            ],
            schema(int_schema('int')),
        ));
    }

    public function test_least_value(): void
    {
        $lest = least(10, 20, ref('int'), 40);

        static::assertSame(10, (new FunctionContext(flow_context()))->eval(
            $lest,
            ['int' => 55],
            schema(int_schema('int')),
        ));
    }

    public function test_least_with_null(): void
    {
        static::assertSame(4, (new FunctionContext(flow_context()))->eval(
            least(null, 20, ref('int'), 1257),
            [
                'int' => 4,
            ],
            schema(int_schema('int')),
        ));
    }

    public function test_least_orders_strings_by_bytes(): void
    {
        static::assertSame('10', (new FunctionContext(flow_context()))->eval(
            least(ref('a'), lit('10'), lit('9')),
            ['a' => null],
            schema(str_schema('a', nullable: true)),
        ));
    }

    public function test_least_orders_a_numeric_string_column_numerically(): void
    {
        static::assertSame('9', (new FunctionContext(flow_context()))->eval(
            least(ref('a'), ref('b')),
            ['a' => '9', 'b' => '10'],
            schema(definition_from_type('a', type_numeric_string()), definition_from_type('b', type_numeric_string())),
        ));
    }
}
