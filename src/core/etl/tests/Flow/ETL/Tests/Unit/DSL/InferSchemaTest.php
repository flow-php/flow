<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\DSL;

use Flow\ETL\Schema\Inference\SchemaInferenceBuilder;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\infer_schema;

final class InferSchemaTest extends FlowTestCase
{
    public function test_each_call_returns_a_fresh_builder_because_the_builder_is_mutable(): void
    {
        static::assertNotSame(infer_schema(), infer_schema());
        static::assertSame(20_480, infer_schema()->build()->sampleSize);
    }

    public function test_infer_schema_delegates_to_the_builder(): void
    {
        static::assertInstanceOf(SchemaInferenceBuilder::class, infer_schema());
    }
}
