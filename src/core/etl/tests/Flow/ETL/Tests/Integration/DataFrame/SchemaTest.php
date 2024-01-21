<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use Flow\ETL\Tests\Integration\IntegrationTestCase;

final class SchemaTest extends IntegrationTestCase
{
    public function test_getting_schema() : void
    {
        $rows = array_to_rows(\array_map(
            function ($i) {
                return [
                    'id' => $i,
                    'name' => 'name_' . $i,
                    'active' => $i % 2 === 0,
                ];
            },
            \range(1, 100)
        ));

        $this->assertEquals(
            schema(
                int_schema('id'),
                str_schema('name'),
                bool_schema('active')
            ),
            df()
                ->read(from_rows($rows))
                ->autoCast()
                ->schema()
        );
    }

    public function test_getting_schema_from_limited_rows() : void
    {
        $rows = array_to_rows(\array_map(
            function ($i) {
                return [
                    'id' => $i,
                    'name' => 'name_' . $i,
                    'active' => $i % 2 === 0,
                    'union' => $i > 50 ? 'string' : 1,
                ];
            },
            \range(1, 100)
        ));

        $this->assertEquals(
            schema(
                int_schema('id'),
                str_schema('name'),
                bool_schema('active'),
                int_schema('union')
            ),
            df()
                ->read(from_rows($rows))
                ->autoCast()
                ->limit(50)
                ->schema()
        );
    }
}
