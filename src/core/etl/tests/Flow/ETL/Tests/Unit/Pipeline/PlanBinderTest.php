<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use Flow\ETL\DataFrame;
use Flow\ETL\DataFrameFactory;
use Flow\ETL\Exception\DataDependentSchemaException;
use Flow\ETL\Join\Expression;
use Flow\ETL\Pipeline\PlanBinder;
use Flow\ETL\Pipeline\Segments;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;
use Flow\ETL\Transformer\AddRowIndexTransformer;
use Flow\ETL\Transformer\JoinEachRowsTransformer;
use Flow\ETL\Transformer\SelectEntriesTransformer;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\to_array;

final class PlanBinderTest extends FlowTestCase
{
    public function test_a_loader_is_passed_through_untouched(): void
    {
        $segments = new Segments();
        $output = [];
        $segments->add($loader = to_array($output));

        $bound = (new PlanBinder())->bind(new CountingExtractor(schema(int_schema('id'))), $segments);

        static::assertSame([$loader], $bound->segments()->steps());
        static::assertEquals(schema(int_schema('id')), $bound->schema);
    }

    public function test_a_step_that_cannot_describe_its_output_refuses_the_walk(): void
    {
        $segments = new Segments();
        $segments->add(JoinEachRowsTransformer::inner(
            new class implements DataFrameFactory {
                public function from(Rows $rows): DataFrame
                {
                    return data_frame()->process(rows(schema(str_schema('code')), row(['code' => 'PL'])));
                }
            },
            Expression::on(['country' => 'code'], 'joined_'),
        ));

        $this->expectException(DataDependentSchemaException::class);
        $this->expectExceptionMessage("its right side is built from each left batch's row values");

        (new PlanBinder())->bind(new CountingExtractor(schema(str_schema('country'))), $segments);
    }

    public function test_the_extractor_schema_seeds_the_walk(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('name')),
            (new PlanBinder())->bind(
                new CountingExtractor(schema(int_schema('id'), str_schema('name'))),
                new Segments(),
            )->schema,
        );
    }

    public function test_the_output_of_a_two_step_chain_is_the_second_steps(): void
    {
        $segments = new Segments();
        $segments->add(new AddRowIndexTransformer('index', StartFrom::ZERO));
        $segments->add(new SelectEntriesTransformer('index'));

        static::assertEquals(
            schema(int_schema('index')),
            (new PlanBinder())->bind(
                new CountingExtractor(schema(int_schema('id'), str_schema('name'))),
                $segments,
            )->schema,
        );
    }

    public function test_the_walk_reads_no_row(): void
    {
        $segments = new Segments();
        $segments->add(new SelectEntriesTransformer('id'));

        (new PlanBinder())->bind($extractor = new CountingExtractor(schema(int_schema('id'))), $segments);

        static::assertSame(0, $extractor->extractCalls);
    }
}
