<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\Literal;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\schema;

final class LiteralTest extends FlowTestCase
{
    public function test_constructor_rejects_a_closure_nested_in_an_array(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'A Closure cannot be used as a literal value: pipeline objects must not hold executable state.',
        );

        new Literal(['handlers' => [static fn(): int => 1]]);
    }

    public function test_constructor_rejects_a_closure(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'A Closure cannot be used as a literal value: pipeline objects must not hold executable state.',
        );

        new Literal(static fn(): int => 1);
    }

    public function test_a_batch_of_the_same_size_reuses_the_column(): void
    {
        $literal = lit(7);
        $context = flow_context();
        $first = $literal->eval(array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))), $context);

        static::assertSame($first, $literal->eval(array_to_rows([
            ['id' => 3],
            ['id' => 4],
        ], schema(int_schema('id'))), $context));
        static::assertSame([7, 7], $first->values());
    }

    public function test_a_batch_of_another_size_gets_a_column_of_its_size(): void
    {
        $literal = lit('x');
        $two = $literal->eval(array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))), flow_context());
        $three = $literal->eval(array_to_rows([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
        ], schema(int_schema('id'))), flow_context());

        static::assertSame(['x', 'x'], $two->values());
        static::assertSame(['x', 'x', 'x'], $three->values());
        static::assertSame(
            ['x', 'x'],
            $literal
                ->eval(array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))), flow_context())
                ->values(),
        );
    }

    public function test_the_column_is_a_constant_of_the_configured_backend(): void
    {
        $backend = new SpyBackend();

        lit(5)->eval(RowsMother::sequentialIds(3), flow_context(config_builder()->backend($backend)->build()));

        static::assertSame(1, $backend->constants());
    }

    public function test_a_batch_of_the_same_size_in_another_backend_gets_a_column_of_that_backend(): void
    {
        $literal = lit(5);
        $first = new SpyBackend();
        $second = new SpyBackend();

        $literal->eval(RowsMother::sequentialIds(3), flow_context(config_builder()->backend($first)->build()));
        $literal->eval(RowsMother::sequentialIds(3), flow_context(config_builder()->backend($second)->build()));

        static::assertSame(1, $first->constants());
        static::assertSame(1, $second->constants());
    }
}
