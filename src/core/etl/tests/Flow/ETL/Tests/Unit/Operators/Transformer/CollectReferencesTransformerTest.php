<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Transformer;

use Flow\ETL\Operators\Transformer\CollectReferencesTransformer;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\StepRoundTrip;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class CollectReferencesTransformerTest extends FlowTestCase
{
    public function test_bind_returns_the_input_schema(): void
    {
        $input = schema(int_schema('id'), str_schema('name'));

        static::assertEquals($input, (new CollectReferencesTransformer(refs()))->bind($input)->output);
    }

    public function test_collecting_references_from_the_batch_schema(): void
    {
        $references = refs();
        $batch = array_to_rows([['id' => 1, 'name' => 'John']], schema(int_schema('id'), str_schema('name')));

        static::assertSame($batch, (new CollectReferencesTransformer($references))->transform(
            $batch,
            flow_context()->backend(),
        ));
        static::assertSame(
            ['id', 'name'],
            array_map(static fn($reference): string => $reference->name(), $references->all()),
        );
    }

    public function test_describe_round_trips(): void
    {
        $transformer = new CollectReferencesTransformer(refs('id', ref('name')->as('n')));
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
