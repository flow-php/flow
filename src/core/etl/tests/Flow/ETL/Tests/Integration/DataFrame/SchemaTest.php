<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\{array_to_rows,
    bool_entry,
    bool_schema,
    df,
    float_entry,
    float_schema,
    flow_context,
    from_array,
    from_rows,
    int_entry,
    int_schema,
    null_entry,
    row,
    rows,
    schema,
    str_schema,
    string_entry};
use Flow\ETL\Pipeline\SynchronousPipeline;
use Flow\ETL\Row\Schema;
use Flow\ETL\Tests\FlowIntegrationTestCase;

final class SchemaTest extends FlowIntegrationTestCase
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
            fn ($i) => [
                'id' => $i,
                'name' => 'name_' . $i,
                'active' => $i % 2 === 0,
            ],
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
            fn ($i) => [
                'id' => $i,
                'name' => 'name_' . $i,
                'active' => $i % 2 === 0,
                'union' => $i > 50 ? 'string' : 1,
            ],
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

    public function test_schema_when_starting_rows_are_null() : void
    {
        $rows = df()
            ->read(from_array(
                [
                    ['string' => null, 'bool' => null, 'int' => null, 'float' => null],
                    ['string' => 'a', 'bool' => true, 'int' => 1, 'float' => 1.24],
                ],
            ))
            ->collect()
            ->fetch();

        self::assertEquals(
            schema(
                str_schema('string', true),
                bool_schema('bool', true),
                int_schema('int', true),
                float_schema('float', true),
            ),
            $rows->schema()
        );
    }

    public function test_taking_schema_from_pipeline() : void
    {
        $pipeline = new SynchronousPipeline(
            $extractor = from_array(
                [
                    ['string' => null, 'bool' => null, 'int' => null, 'float' => null],
                    ['string' => 'a', 'bool' => true, 'int' => 1, 'float' => 1.24],
                ],
            )
        );

        self::assertEquals(
            schema(
                str_schema('string', true),
                bool_schema('bool', true),
                int_schema('int', true),
                float_schema('float', true),
            ),
            Schema::fromPipeline($pipeline, $context = flow_context())
        );

        self::assertEquals(
            [
                rows(row(null_entry('string'), null_entry('bool'), null_entry('int'), null_entry('float'))),
                rows(row(string_entry('string', 'a'), bool_entry('bool', true), int_entry('int', 1), float_entry('float', 1.24))),
            ],
            iterator_to_array($extractor->extract($context))
        );
    }
}
