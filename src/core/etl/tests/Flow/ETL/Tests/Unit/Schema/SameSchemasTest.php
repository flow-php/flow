<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema;

use Flow\ETL\Schema\SameSchemas;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class SameSchemasTest extends FlowTestCase
{
    public function test_a_pair_is_proved_only_once_remembered_and_only_in_that_direction(): void
    {
        $schema = schema(int_schema('id'));
        $other = schema(int_schema('id'));

        static::assertFalse(SameSchemas::proved($schema, $other));

        SameSchemas::remember($schema, $other);

        static::assertTrue(SameSchemas::proved($schema, $other));
        static::assertFalse(SameSchemas::proved($other, $schema));
        static::assertFalse(SameSchemas::proved($schema, schema(int_schema('id'))));
    }
}
