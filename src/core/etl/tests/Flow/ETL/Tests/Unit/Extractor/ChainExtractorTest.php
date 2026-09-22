<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Cardinality;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\BatchableExtractor;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\FlowContext;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Schema;
use Flow\ETL\Tests\Double\DeclaringExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Generator;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_data_frame;
use function Flow\ETL\DSL\from_memory;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

final class ChainExtractorTest extends FlowTestCase
{
    public function test_a_chain_containing_a_data_frame_source_folds_its_schema(): void
    {
        static::assertEquals(
            schema(int_schema('id', true), str_schema('name', true)),
            from_all(
                from_rows(rows(schema(int_schema('id')), row(['id' => 1]))),
                from_data_frame(df()->read(from_rows(rows(schema(str_schema('name')), row(['name' => 'a']))))),
            )->schema(),
        );
    }

    public function test_a_chain_takes_no_batch_size_of_its_own(): void
    {
        // a chain has no unit of its own: batches(from_all(...), $n) re-slices one, or each child is sized
        static::assertNotInstanceOf(BatchableExtractor::class, from_all(from_rows(rows(schema()))));
    }

    public function test_chain_extractor(): void
    {
        $extractor = from_all(new class implements Extractor {
            public function withSchema(Schema $schema): static
            {
                return $this;
            }

            public function schema(): Schema
            {
                return schema(int_schema('id'));
            }

            public function extract(FlowContext $context, ?int $limit = null): Generator
            {
                yield rows(schema(int_schema('id')), row(['id' => 1]));
                yield rows(schema(int_schema('id')), row(['id' => 2]));
            }

            public function statistics(): Statistics
            {
                return new Statistics();
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

            public function extract(FlowContext $context, ?int $limit = null): Generator
            {
                yield rows(schema(int_schema('id')), row(['id' => 3]));
                yield rows(schema(int_schema('id')), row(['id' => 4]));
            }

            public function statistics(): Statistics
            {
                return new Statistics();
            }
        });

        self::assertExtractedRowsEquals(
            rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]), row(['id' => 4])),
            $extractor,
        );
    }

    public function test_with_schema_does_not_leak_into_a_second_pipeline(): void
    {
        $child = from_rows(rows(schema(int_schema('id')), row(['id' => 1])));

        iterator_to_array(
            from_all($child)
                ->withSchema(schema(int_schema('id'), str_schema('name', nullable: true)))
                ->extract(flow_context()),
            false,
        );

        static::assertTrue($child->schema()->isSame(schema(int_schema('id'))));
        static::assertSame(
            [['id' => 1]],
            iterator_to_array(from_all($child)->extract(flow_context()), false)[0]->toArray(),
        );
    }

    public function test_is_repeatable(): void
    {
        static::assertTrue(from_all(from_rows(rows(schema(int_schema('id')), row(['id' => 1]))))->isRepeatable());
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
            from_all(from_array([['id' => 1], ['id' => 2]]), from_memory(new ArrayMemory([['id' => 3]])))->statistics(),
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
