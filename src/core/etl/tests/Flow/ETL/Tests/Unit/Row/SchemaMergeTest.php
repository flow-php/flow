<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use function Flow\ETL\DSL\{bool_schema, datetime_schema, float_schema, int_schema, object_schema, schema, str_schema, type_object};
use PHPUnit\Framework\TestCase;

final class SchemaMergeTest extends TestCase
{
    public function test_merge_different_schemas() : void
    {
        $schema = (schema(
            int_schema('id'),
            str_schema('name', nullable: true)
        ))->merge(
            schema(
                bool_schema('test'),
            )
        );

        self::assertEquals(
            schema(
                int_schema('id', nullable: true),
                str_schema('name', nullable: true),
                bool_schema('test', nullable: true),
            ),
            $schema
        );
    }

    public function test_merge_different_schemas_with_common_parts() : void
    {
        $schema = (schema(
            int_schema('id'),
            str_schema('name')
        ))->merge(
            schema(
                bool_schema('test'),
                str_schema('name')
            )
        );

        self::assertEquals(
            schema(
                int_schema('id', nullable: true),
                str_schema('name'),
                bool_schema('test', nullable: true),
            ),
            $schema
        );
    }

    public function test_merge_different_schemas_with_common_parts_but_different_nullable_definitions() : void
    {
        $schema = (schema(
            int_schema('id'),
            str_schema('name', nullable: true)
        ))->merge(
            schema(
                bool_schema('test'),
                str_schema('name', nullable: false)
            )
        );

        self::assertEquals(
            schema(
                int_schema('id', nullable: true),
                str_schema('name', nullable: true),
                bool_schema('test', nullable: true),
            ),
            $schema
        );
    }

    public function test_merge_int_empty_schema() : void
    {
        $schema = (schema())->merge(
            $notEmptySchema = schema(
                int_schema('id', nullable: true),
                str_schema('name', nullable: true)
            )
        );

        self::assertSame(
            $notEmptySchema,
            $schema
        );
    }

    public function test_merge_schema() : void
    {
        $schema = (schema(
            int_schema('id', nullable: true),
            str_schema('name', nullable: true)
        ))->merge(
            schema(
                str_schema('test'),
            )
        );

        self::assertEquals(
            schema(
                int_schema('id', nullable: true),
                str_schema('name', nullable: true),
                str_schema('test', nullable: true),
            ),
            $schema
        );
    }

    public function test_merge_with_empty_schema() : void
    {
        $schema = ($notEmptySchema = schema(
            int_schema('id', nullable: true),
            str_schema('name', nullable: true)
        ))->merge(
            schema()
        );

        self::assertEquals(
            $notEmptySchema,
            $schema
        );
    }

    public function test_nullable_with_non_nullable_schema() : void
    {
        self::assertEquals(
            schema(str_schema('col', nullable: true)),
            schema(str_schema('col'))->merge(schema(str_schema('col', nullable: true)))
        );
        self::assertEquals(
            schema(int_schema('col', nullable: true)),
            schema(int_schema('col'))->merge(schema(int_schema('col', nullable: true)))
        );
        self::assertEquals(
            schema(bool_schema('col', nullable: true)),
            schema(bool_schema('col'))->merge(schema(bool_schema('col', nullable: true)))
        );
        self::assertEquals(
            schema(float_schema('col', nullable: true)),
            schema(float_schema('col'))->merge(schema(float_schema('col', nullable: true)))
        );
        self::assertEquals(
            schema(object_schema('col', type_object(\stdClass::class, nullable: true))),
            schema(object_schema('col', type_object(\stdClass::class)))->merge(schema(object_schema('col', type_object(\stdClass::class, nullable: true))))
        );
        self::assertEquals(
            schema(datetime_schema('col', nullable: true)),
            schema(datetime_schema('col'))->merge(schema(datetime_schema('col', nullable: true)))
        );
    }
}
