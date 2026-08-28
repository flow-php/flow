<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeImmutable;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\least;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;

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
        static::assertNull(least(null, null)->eval(row(int_entry('int', 4)), flow_context()));
    }

    public function test_least_value(): void
    {
        $lest = least(10, 20, ref('int'), 40);

        static::assertSame(10, $lest->eval(row(int_entry('int', 55)), flow_context()));
    }

    public function test_least_with_null(): void
    {
        static::assertSame(4, least(null, 20, ref('int'), 1257)->eval(row(int_entry('int', 4)), flow_context()));
    }
}
