<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\coalesce;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class CoalesceTest extends FlowTestCase
{
    public function test_a_non_nullable_branch_makes_the_result_not_nullable(): void
    {
        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve(
            coalesce(ref('name'), lit('N/A')),
            schema(str_schema('name', nullable: true)),
        );

        static::assertSame('string', $resolved->returns()->toString());
    }

    public function test_coalesce_entries(): void
    {
        static::assertSame(1, coalesce(ref('name'), ref('id'), lit('N/A'))->eval(row([
            'name' => null,
            'id' => 1,
        ]), flow_context()));
    }

    public function test_coalesce_on_null_entries_falls_through_to_lit(): void
    {
        static::assertSame('N/A', coalesce(ref('name'), ref('string'), lit('N/A'))->eval(row([
            'name' => null,
            'string' => null,
        ]), flow_context()));
    }

    public function test_coalesce_on_ref(): void
    {
        static::assertSame(1, ref('name')
            ->coalesce(ref('id'), lit('N/A'))
            ->eval(row(['name' => null, 'id' => 1]), flow_context()));
    }

    public function test_a_throwing_branch_is_not_swallowed(): void
    {
        $this->expectException(InvalidArgumentException::class);

        coalesce(ref('a')->upper(), ref('b')->upper())->eval(row(['a' => 1, 'b' => 2]), flow_context());
    }
}
