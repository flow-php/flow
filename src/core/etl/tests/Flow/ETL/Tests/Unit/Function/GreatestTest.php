<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeImmutable;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\greatest;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;

final class GreatestTest extends FlowTestCase
{
    public function test_a_non_nullable_operand_makes_the_result_not_nullable(): void
    {
        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve(
            greatest(ref('int'), lit(10)),
            schema(int_schema('int', nullable: true)),
        );

        static::assertSame('integer', $resolved->returns()->toString());
    }

    public function test_greatest_value(): void
    {
        $greatest = greatest(10, 20, ref('int'), 40);

        static::assertSame(55, $greatest->eval(row(['int' => 55]), flow_context()));
    }

    public function test_greatest_with_non_comparable_values(): void
    {
        $this->expectExceptionMessage('Cannot combine types');

        greatest(null, 20, lit(new DateTimeImmutable('now')))->returns();
    }

    public function test_an_integer_and_a_string_column_have_no_common_type(): void
    {
        $this->expectExceptionMessage('Cannot combine types "integer", "string"');

        greatest(5, 'apple')->returns();
    }

    public function test_nulls_are_skipped(): void
    {
        static::assertSame(20, greatest(null, 20, ref('int'))->eval(row(['int' => 4]), flow_context()));
    }

    public function test_null_only_when_every_argument_is_null(): void
    {
        static::assertNull(greatest(null, null)->eval(row(['int' => 4]), flow_context()));
    }

    public function test_greatest_with_null(): void
    {
        $greatest = greatest(null, 20, ref('int'), 1257);

        static::assertSame(1257, $greatest->eval(row(['int' => 55]), flow_context()));
    }
}
