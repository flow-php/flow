<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Tests\Context\BackendsContext;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\PhpBackend;
use PHPUnit\Framework\Attributes\DataProviderExternal;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\Types\DSL\type_boolean;

final class IsNullTest extends TestCase
{
    #[DataProviderExternal(BackendsContext::class, 'backends')]
    public function test_answers_per_row(Backend $backend): void
    {
        $column = (new FunctionContext(flow_context(config_builder()->backend($backend)->build())))->column(
            ref('a')->isNull(),
            [['a' => 1], ['a' => null], ['a' => 3]],
            schema(int_schema('a', nullable: true)),
        );

        static::assertSame([false, true, false], $column->values());
        static::assertEquals(type_boolean(), $column->type());
        static::assertSame(0, $column->nullCount());
    }

    #[DataProviderExternal(BackendsContext::class, 'backends')]
    public function test_a_column_without_nulls_answers(Backend $backend): void
    {
        $column = (new FunctionContext(flow_context(config_builder()->backend($backend)->build())))->column(
            ref('a')->isNull(),
            [['a' => 1], ['a' => 2]],
            schema(int_schema('a', nullable: true)),
        );

        static::assertSame([false, false], $column->values());
        static::assertEquals(type_boolean(), $column->type());
    }

    public function test_no_rows_answer_with_an_empty_column(): void
    {
        static::assertSame(
            0,
            (new FunctionContext(flow_context(config_builder()->backend(new PhpBackend())->build())))
                ->column(ref('a')->isNull(), [], schema(int_schema('a', nullable: true)))
                ->count(),
        );
    }
}
