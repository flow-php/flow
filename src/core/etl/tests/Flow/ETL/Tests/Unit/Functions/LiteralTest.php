<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use DateTimeImmutable;
use DOMDocument;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Literal;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;

use function Flow\ETL\DSL\array_to_rows;
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
        $first = $literal->eval(array_to_rows([
            ['id' => 1],
            ['id' => 2],
        ], schema(int_schema('id'))), $context->backend());

        static::assertSame($first, $literal->eval(array_to_rows([
            ['id' => 3],
            ['id' => 4],
        ], schema(int_schema('id'))), $context->backend()));
        static::assertSame([7, 7], $first->values());
    }

    public function test_a_batch_of_another_size_gets_a_column_of_its_size(): void
    {
        $literal = lit('x');
        $two = $literal->eval(array_to_rows([
            ['id' => 1],
            ['id' => 2],
        ], schema(int_schema('id'))), flow_context()->backend());
        $three = $literal->eval(array_to_rows([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
        ], schema(int_schema('id'))), flow_context()->backend());

        static::assertSame(['x', 'x'], $two->values());
        static::assertSame(['x', 'x', 'x'], $three->values());
        static::assertSame(
            ['x', 'x'],
            $literal
                ->eval(array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))), flow_context()->backend())
                ->values(),
        );
    }

    public function test_the_column_is_a_constant_of_the_configured_backend(): void
    {
        $backend = new SpyBackend();

        lit(5)->eval(RowsMother::sequentialIds(3), $backend);

        static::assertSame(1, $backend->constants());
    }

    public function test_a_batch_of_the_same_size_in_another_backend_gets_a_column_of_that_backend(): void
    {
        $literal = lit(5);
        $first = new SpyBackend();
        $second = new SpyBackend();

        $literal->eval(RowsMother::sequentialIds(3), $first);
        $literal->eval(RowsMother::sequentialIds(3), $second);

        static::assertSame(1, $first->constants());
        static::assertSame(1, $second->constants());
    }

    public function test_describe_round_trips(): void
    {
        $context = new FunctionContext();
        $literal = lit(['a' => new DateTimeImmutable('2026-01-02 03:04:05 UTC'), 'b' => [1, 2.5]]);
        $schema = schema(int_schema('id'));
        $rows = RowsMother::sequentialIds(2);

        static::assertSame(
            $context->exported($literal, $rows, $schema),
            $context->exported(DescribeRoundTrip::rebuilt($literal), $rows, $schema),
        );
    }

    public function test_a_dom_literal_does_not_describe(): void
    {
        $document = new DOMDocument();
        $document->loadXML('<root><a>1</a></root>');

        static::assertNull(lit($document->documentElement)->describe());
    }
}
