<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\Row\{Schema};
use PHPUnit\Framework\TestCase;

final class SchemaMergeTest extends TestCase
{
    public function test_merge_different_schemas() : void
    {
        $schema = (new Schema(
            Schema\Definition::integer('id'),
            Schema\Definition::string('name', nullable: true)
        ))->merge(
            new Schema(
                Schema\Definition::boolean('test'),
            )
        );

        self::assertEquals(
            new Schema(
                Schema\Definition::integer('id', nullable: true),
                Schema\Definition::string('name', nullable: true),
                Schema\Definition::boolean('test', nullable: true),
            ),
            $schema
        );
    }

    public function test_merge_different_schemas_with_common_parts() : void
    {
        $schema = (new Schema(
            Schema\Definition::integer('id'),
            Schema\Definition::string('name')
        ))->merge(
            new Schema(
                Schema\Definition::boolean('test'),
                Schema\Definition::string('name')
            )
        );

        self::assertEquals(
            new Schema(
                Schema\Definition::integer('id', nullable: true),
                Schema\Definition::string('name'),
                Schema\Definition::boolean('test', nullable: true),
            ),
            $schema
        );
    }

    public function test_merge_different_schemas_with_common_parts_but_different_nullable_definitions() : void
    {
        $schema = (new Schema(
            Schema\Definition::integer('id'),
            Schema\Definition::string('name', nullable: true)
        ))->merge(
            new Schema(
                Schema\Definition::boolean('test'),
                Schema\Definition::string('name', nullable: false)
            )
        );

        self::assertEquals(
            new Schema(
                Schema\Definition::integer('id', nullable: true),
                Schema\Definition::string('name', nullable: true),
                Schema\Definition::boolean('test', nullable: true),
            ),
            $schema
        );
    }

    public function test_merge_int_empty_schema() : void
    {
        $schema = (new Schema())->merge(
            $notEmptySchema = new Schema(
                Schema\Definition::integer('id', nullable: true),
                Schema\Definition::string('name', nullable: true)
            )
        );

        self::assertSame(
            $notEmptySchema,
            $schema
        );
    }

    public function test_merge_schema() : void
    {
        $schema = (new Schema(
            Schema\Definition::integer('id', nullable: true),
            Schema\Definition::string('name', nullable: true)
        ))->merge(
            new Schema(
                Schema\Definition::string('test'),
            )
        );

        self::assertEquals(
            new Schema(
                Schema\Definition::integer('id', nullable: true),
                Schema\Definition::string('name', nullable: true),
                Schema\Definition::string('test', nullable: true),
            ),
            $schema
        );
    }

    public function test_merge_with_empty_schema() : void
    {
        $schema = ($notEmptySchema = new Schema(
            Schema\Definition::integer('id', nullable: true),
            Schema\Definition::string('name', nullable: true)
        ))->merge(
            new Schema()
        );

        self::assertEquals(
            $notEmptySchema,
            $schema
        );
    }
}
