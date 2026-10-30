<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Transformer;

use Flow\ETL\Foundation\Exception\SchemaDefinitionNotUniqueException;
use Flow\ETL\Operators\Transformer\AddRowIndex\StartFrom;
use Flow\ETL\Operators\Transformer\AddRowIndexTransformer;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\StepRoundTrip;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class AddRowIndexTransformerTest extends FlowTestCase
{
    public function test_an_index_column_colliding_with_an_existing_column_is_rejected(): void
    {
        $this->expectException(SchemaDefinitionNotUniqueException::class);
        $this->expectExceptionMessage('Column definitions must be unique, duplicated columns: [id]');

        (new AddRowIndexTransformer('id', StartFrom::ZERO))->transform(array_to_rows([[
            'id' => 1,
        ]], schema(int_schema('id'))), flow_context()->backend());
    }

    public function test_bind_adds_an_int_index_column(): void
    {
        static::assertEquals(
            schema(int_schema('id'), int_schema('idx')),
            (new AddRowIndexTransformer('idx', StartFrom::ZERO))->bind(schema(int_schema('id')))->output,
        );
    }

    public function test_index_keeps_incrementing_across_batches(): void
    {
        $transformer = new AddRowIndexTransformer('idx', StartFrom::ZERO);
        $schema = schema(int_schema('id'));
        $context = flow_context();

        static::assertSame(
            [['id' => 1, 'idx' => 0], ['id' => 2, 'idx' => 1]],
            $transformer->transform(array_to_rows([['id' => 1], ['id' => 2]], $schema), $context->backend())->toArray(),
        );
        static::assertSame(
            [['id' => 3, 'idx' => 2]],
            $transformer->transform(array_to_rows([['id' => 3]], $schema), $context->backend())->toArray(),
        );
    }

    #[TestWith([StartFrom::ZERO, 0])]
    #[TestWith([StartFrom::ONE, 1])]
    public function test_a_fresh_instance_starts_counting_again(StartFrom $startFrom, int $first): void
    {
        $transformer = new AddRowIndexTransformer('idx', $startFrom);
        $schema = schema(int_schema('id'));
        $transformer->transform(array_to_rows([['id' => 1], ['id' => 2]], $schema), flow_context()->backend());

        $fresh = $transformer->fresh();

        static::assertNotSame($transformer, $fresh);
        static::assertSame(
            [['id' => 1, 'idx' => $first]],
            $fresh->transform(array_to_rows([['id' => 1]], $schema), flow_context()->backend())->toArray(),
        );
    }

    public function test_index_starting_from_one(): void
    {
        static::assertSame(
            [['id' => 1, 'idx' => 1]],
            (new AddRowIndexTransformer('idx', StartFrom::ONE))
                ->transform(array_to_rows([['id' => 1]], schema(int_schema('id'))), flow_context()->backend())
                ->toArray(),
        );
    }

    public function test_describe_round_trips(): void
    {
        $transformer = new AddRowIndexTransformer('idx', StartFrom::ONE);
        $rebuilt = DescribeRoundTrip::rebuilt($transformer);
        $rows = array_to_rows(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b'], ['id' => 3, 'name' => 'c']],
            schema(int_schema('id'), str_schema('name')),
        );
        $schema = schema(int_schema('id'), str_schema('name'));

        static::assertSame(
            StepRoundTrip::transformed($transformer, $schema, $rows, flow_context()),
            StepRoundTrip::transformed($rebuilt, $schema, $rows, flow_context()),
        );
    }
}
