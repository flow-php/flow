<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Foundation\Exception\SchemaValidationException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\match_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class MatchSchemaTest extends FlowTestCase
{
    public function test_match_schema_through_invoke(): void
    {
        $this->expectException(SchemaValidationException::class);

        match_schema(schema(str_schema('a')))(df()->read(from_array([['a' => 1]])))->fetch();
    }

    public function test_match_schema_through_with(): void
    {
        $this->expectException(SchemaValidationException::class);

        df()
            ->read(from_array([['a' => 1]]))
            ->with(match_schema(schema(str_schema('a'))))
            ->fetch();
    }
}
