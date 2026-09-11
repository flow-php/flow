<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\CollectReferencesTransformer;

use function array_map;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
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
        $batch = rows(schema(int_schema('id'), str_schema('name')), row(['id' => 1, 'name' => 'John']));

        static::assertSame($batch, (new CollectReferencesTransformer($references))->transform($batch, flow_context()));
        static::assertSame(
            ['id', 'name'],
            array_map(static fn($reference): string => $reference->name(), $references->all()),
        );
    }
}
