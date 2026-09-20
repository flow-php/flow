<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Exception\SchemaDefinitionNotUniqueException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;
use Flow\ETL\Transformer\AddRowIndexTransformer;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class AddRowIndexTransformerTest extends FlowTestCase
{
    public function test_an_index_column_colliding_with_an_existing_column_is_rejected(): void
    {
        $this->expectException(SchemaDefinitionNotUniqueException::class);
        $this->expectExceptionMessage('Entry definitions must be unique, duplicated entries: [id]');

        (new AddRowIndexTransformer('id', StartFrom::ZERO))->transform(
            rows(schema(int_schema('id')), row(['id' => 1])),
            flow_context(),
        );
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
            $transformer->transform(rows($schema, row(['id' => 1]), row(['id' => 2])), $context)->toArray(),
        );
        static::assertSame(
            [['id' => 3, 'idx' => 2]],
            $transformer->transform(rows($schema, row(['id' => 3])), $context)->toArray(),
        );
    }

    #[TestWith([StartFrom::ZERO, 0])]
    #[TestWith([StartFrom::ONE, 1])]
    public function test_a_fresh_instance_starts_counting_again(StartFrom $startFrom, int $first): void
    {
        $transformer = new AddRowIndexTransformer('idx', $startFrom);
        $schema = schema(int_schema('id'));
        $transformer->transform(rows($schema, row(['id' => 1]), row(['id' => 2])), flow_context());

        $fresh = $transformer->fresh();

        static::assertNotSame($transformer, $fresh);
        static::assertSame(
            [['id' => 1, 'idx' => $first]],
            $fresh->transform(rows($schema, row(['id' => 1])), flow_context())->toArray(),
        );
    }

    public function test_index_starting_from_one(): void
    {
        static::assertSame(
            [['id' => 1, 'idx' => 1]],
            (new AddRowIndexTransformer('idx', StartFrom::ONE))
                ->transform(rows(schema(int_schema('id')), row(['id' => 1])), flow_context())
                ->toArray(),
        );
    }
}
