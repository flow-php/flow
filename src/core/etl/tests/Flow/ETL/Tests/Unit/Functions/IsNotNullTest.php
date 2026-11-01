<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Column\RustBackend;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\Types\DSL\type_boolean;

final class IsNotNullTest extends TestCase
{
    public function test_answers_per_row_in_php_backend(): void
    {
        $column = (new FunctionContext(flow_context(config_builder()->backend(new PhpBackend())->build())))->column(
            ref('a')->isNotNull(),
            [['a' => 1], ['a' => null], ['a' => 3]],
            schema(int_schema('a', nullable: true)),
        );

        static::assertSame([true, false, true], $column->values());
        static::assertEquals(type_boolean(), $column->type());
        static::assertSame(0, $column->nullCount());
    }

    #[RequiresPhpExtension('flow_php')]
    public function test_answers_per_row_in_rust_backend(): void
    {
        $column = (new FunctionContext(flow_context(config_builder()->backend(new RustBackend())->build())))->column(
            ref('a')->isNotNull(),
            [['a' => 1], ['a' => null], ['a' => 3]],
            schema(int_schema('a', nullable: true)),
        );

        static::assertSame([true, false, true], $column->values());
        static::assertEquals(type_boolean(), $column->type());
        static::assertSame(0, $column->nullCount());
    }

    #[RequiresPhpExtension('flow_php')]
    public function test_a_column_without_nulls_answers_in_rust_backend(): void
    {
        $column = (new FunctionContext(flow_context(config_builder()->backend(new RustBackend())->build())))->column(
            ref('a')->isNotNull(),
            [['a' => 1], ['a' => 2]],
            schema(int_schema('a', nullable: true)),
        );

        static::assertSame([true, true], $column->values());
        static::assertEquals(type_boolean(), $column->type());
    }

    public function test_no_rows_answer_with_an_empty_column(): void
    {
        static::assertSame(
            0,
            (new FunctionContext(flow_context(config_builder()->backend(new PhpBackend())->build())))
                ->column(ref('a')->isNotNull(), [], schema(int_schema('a', nullable: true)))
                ->count(),
        );
    }
}
