<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\{array_to_rows, bool_schema, df, from_array, from_rows, int_schema, schema, str_schema};
use Flow\ETL\Tests\Integration\IntegrationTestCase;

final class SchemaTest extends IntegrationTestCase
{
    public function test_extraction_according_to_schema() : void
    {
        $rows = df()
            ->read(from_array(
                [
                    ['id' => 1, 'name' => 'name_1', 'active' => null],
                    ['id' => 2, 'name' => 'name_2', 'active' => null],
                    ['id' => 3, 'name' => 'name_3', 'active' => null],
                ],
                $schema = schema(
                    int_schema('id'),
                    str_schema('name'),
                    bool_schema('active', nullable: true)
                )
            ))
            ->collect()
            ->fetch();

        self::assertEquals(
            [
                ['id' => 1, 'name' => 'name_1', 'active' => null],
                ['id' => 2, 'name' => 'name_2', 'active' => null],
                ['id' => 3, 'name' => 'name_3', 'active' => null],
            ],
            $rows->toArray()
        );
        self::assertEquals(
            $schema,
            $rows->schema()
        );
    }

    public function test_extraction_without_to_schema() : void
    {
        $rows = df()
            ->read(from_array(
                [
                    ['id' => 1, 'name' => 'name_1', 'active' => null],
                    ['id' => 2, 'name' => 'name_2', 'active' => null],
                    ['id' => 3, 'name' => 'name_3', 'active' => null],
                ],
            ))
            ->collect()
            ->fetch();

        self::assertEquals(
            [
                ['id' => 1, 'name' => 'name_1', 'active' => null],
                ['id' => 2, 'name' => 'name_2', 'active' => null],
                ['id' => 3, 'name' => 'name_3', 'active' => null],
            ],
            $rows->toArray()
        );
        self::assertEquals(
            schema(
                int_schema('id'),
                str_schema('name'),
                str_schema('active', nullable: true)
            ),
            $rows->schema()
        );
    }

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

        self::assertEquals(
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

        self::assertEquals(
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
