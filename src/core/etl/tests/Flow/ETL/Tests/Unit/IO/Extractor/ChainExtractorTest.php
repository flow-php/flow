<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Extractor;

use ArrayIterator;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\BatchableExtractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Tests\Double\DeclaringExtractor;
use Flow\ETL\Tests\Double\RepeatableExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Schema;
use Generator;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

final class ChainExtractorTest extends FlowTestCase
{
    public function test_a_chain_without_extractors_refuses_to_infer_a_schema(): void
    {
        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessage('from_all() has nothing to infer a schema from: it chains no extractor. Declare the schema with '
        . '->withSchema(...).');

        from_all()->schema();
    }

    public function test_a_chain_without_extractors_answers_a_declared_schema(): void
    {
        static::assertEquals(schema(int_schema('id')), from_all()->withSchema(schema(int_schema('id')))->schema());
    }

    public function test_an_empty_nested_chain_reads_nothing_under_a_declared_schema(): void
    {
        static::assertSame(
            [],
            iterator_to_array(
                from_all(from_all(), from_all(from_array([])))
                    ->withSchema(schema(int_schema('id')))
                    ->extract(flow_context()->backend(), new Criteria()),
                false,
            ),
        );
    }

    public function test_a_declared_chain_schema_reads_an_empty_child_without_asking_it(): void
    {
        static::assertSame(
            [],
            iterator_to_array(
                from_all(from_array([]))
                    ->withSchema(schema(int_schema('id')))
                    ->extract(flow_context()->backend(), new Criteria()),
                false,
            ),
        );
    }

    public function test_a_chain_takes_no_batch_size_of_its_own(): void
    {
        // a chain has no unit of its own: each child is sized, or the frame re-batches the merged stream
        static::assertNotInstanceOf(BatchableExtractor::class, from_all(from_rows(rows(schema()))));
    }

    public function test_chain_extractor(): void
    {
        self::assertExtractedRowsEquals(
            array_to_rows([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4]], schema(int_schema('id'))),
            from_all(new class implements Extractor {
                public function withSchema(Schema $schema): static
                {
                    return $this;
                }

                public function schema(): Schema
                {
                    return schema(int_schema('id'));
                }

                public function extract(Backend $backend, Criteria $criteria): Generator
                {
                    yield array_to_rows([['id' => 1]], schema(int_schema('id')));
                    yield array_to_rows([['id' => 2]], schema(int_schema('id')));
                }

                public function statistics(): Statistics
                {
                    return new Statistics();
                }

                public function isRepeatable(): bool
                {
                    return false;
                }
            }, new class implements Extractor {
                public function withSchema(Schema $schema): static
                {
                    return $this;
                }

                public function schema(): Schema
                {
                    return schema(int_schema('id'));
                }

                public function extract(Backend $backend, Criteria $criteria): Generator
                {
                    yield array_to_rows([['id' => 3]], schema(int_schema('id')));
                    yield array_to_rows([['id' => 4]], schema(int_schema('id')));
                }

                public function statistics(): Statistics
                {
                    return new Statistics();
                }

                public function isRepeatable(): bool
                {
                    return false;
                }
            }),
        );
    }

    public function test_with_schema_does_not_leak_into_a_second_pipeline(): void
    {
        $child = from_rows(array_to_rows([['id' => 1]], schema(int_schema('id'))));

        iterator_to_array(
            from_all($child)
                ->withSchema(schema(int_schema('id'), str_schema('name', nullable: true)))
                ->extract(flow_context()->backend(), new Criteria()),
            false,
        );

        static::assertTrue($child->schema()->isSame(schema(int_schema('id'))));
        static::assertSame(
            [['id' => 1]],
            iterator_to_array(
                from_all($child)->extract(flow_context()->backend(), new Criteria()),
                false,
            )[0]->toArray(),
        );
    }

    public function test_is_repeatable_when_every_child_repeats(): void
    {
        static::assertTrue(from_all(from_rows(array_to_rows([['id' => 1]], schema(int_schema('id')))))->isRepeatable());
        static::assertTrue(
            from_all(new RepeatableExtractor(true), from_all(new RepeatableExtractor(true)))->isRepeatable(),
        );
    }

    public function test_is_not_repeatable_when_a_child_or_a_nested_child_is_not(): void
    {
        static::assertFalse(from_all(new RepeatableExtractor(true), new RepeatableExtractor(false))->isRepeatable());
        static::assertFalse(from_all(from_all(new RepeatableExtractor(false)))->isRepeatable());
    }

    public function test_it_sums_the_statistics_of_its_children(): void
    {
        static::assertEquals(
            new Statistics(rows: Cardinality::exact(5), size: Cardinality::exact(300)),
            from_all(
                new DeclaringExtractor(new Statistics(Cardinality::exact(2), Cardinality::exact(100))),
                new DeclaringExtractor(new Statistics(Cardinality::exact(3), Cardinality::exact(200))),
            )->statistics(),
        );
    }

    public function test_one_unknown_child_makes_the_chain_unknown(): void
    {
        static::assertEquals(
            new Statistics(),
            from_all(
                from_array([
                    ['id' => 1],
                    ['id' => 2],
                ]),
                from_array(new ArrayIterator([['id' => 3]])),
            )->statistics(),
        );
    }

    public function test_one_estimated_child_makes_the_chain_estimated(): void
    {
        $estimated = new DeclaringExtractor(new Statistics(rows: Cardinality::approximately(10, 0.2)));
        $chain = from_all(from_array([['id' => 1], ['id' => 2]]), $estimated);

        static::assertEquals(Cardinality::approximately(12, 0.2), $chain->statistics()->rows);

        $estimated->statistics = new Statistics(rows: Cardinality::exact(10));

        static::assertEquals(Cardinality::exact(12), $chain->statistics()->rows);
    }

    public function test_an_empty_chain_declares_zero_rows_and_bytes_exactly(): void
    {
        static::assertEquals(new Statistics(Cardinality::exact(0), Cardinality::exact(0)), from_all()->statistics());
    }
}
