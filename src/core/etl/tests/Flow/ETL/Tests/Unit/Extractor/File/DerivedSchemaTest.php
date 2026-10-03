<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor\File;

use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Extractor\File\DerivedSchema;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class DerivedSchemaTest extends FlowTestCase
{
    public function test_a_file_of_the_derived_schema_passes(): void
    {
        $this->expectNotToPerformAssertions();

        (new DerivedSchema(schema(int_schema('id')), 'memory://a.floe'))->refuseDivergence(
            'memory://b.floe',
            schema(int_schema('id')),
        );
    }

    public function test_a_file_of_another_schema_is_refused_naming_both_files(): void
    {
        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessage('memory://b.floe');

        (new DerivedSchema(schema(int_schema('id')), 'memory://a.floe'))->refuseDivergence(
            'memory://b.floe',
            schema(str_schema('name')),
        );
    }
}
