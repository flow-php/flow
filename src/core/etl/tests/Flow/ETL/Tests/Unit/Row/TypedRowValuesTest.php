<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\Row\TypedRowValues;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\Types\DSL\type_integer;

final class TypedRowValuesTest extends FlowTestCase
{
    public function test_metadata_defaults_to_empty(): void
    {
        $rowValues = new TypedRowValues(['id' => 1], ['id' => type_integer()]);

        static::assertSame(['id' => 1], $rowValues->values);
        static::assertEquals(['id' => type_integer()], $rowValues->types);
        static::assertSame([], $rowValues->metadata);
    }

    public function test_carries_per_value_types_and_metadata(): void
    {
        $metadata = Metadata::with('source', 'decoder');
        $rowValues = new TypedRowValues(['id' => null], ['id' => type_integer()], ['id' => $metadata]);

        static::assertSame(['id' => null], $rowValues->values);
        static::assertEquals(['id' => type_integer()], $rowValues->types);
        static::assertSame(['id' => $metadata], $rowValues->metadata);
    }
}
