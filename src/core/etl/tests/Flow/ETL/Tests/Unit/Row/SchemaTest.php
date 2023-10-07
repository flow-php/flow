<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Schema;
use PHPUnit\Framework\TestCase;

final class SchemaTest extends TestCase
{
    public function test_allowing_only_unique_definitions() : void
    {
        $this->expectException(InvalidArgumentException::class);

        new Schema(
            Schema\Definition::integer('id'),
            Schema\Definition::string('id')
        );
    }

    public function test_allowing_only_unique_definitions_case_insensitive() : void
    {
        $schema = new Schema(
            Schema\Definition::integer('id'),
            Schema\Definition::integer('Id')
        );

        $this->assertEquals([EntryReference::init('id'), EntryReference::init('Id')], $schema->entries());
    }

    public function test_making_whole_schema_nullable() : void
    {
        $schema = new Schema(
            Schema\Definition::integer('id', $nullable = false),
            Schema\Definition::string('name', $nullable = true)
        );

        $this->assertEquals(
            new Schema(
                Schema\Definition::integer('id', $nullable = true),
                Schema\Definition::string('name', $nullable = true)
            ),
            $schema->nullable()
        );
    }

    public function test_merge_int_empty_schema() : void
    {
        $schema = (new Schema())->merge(
            $notEmptySchema = new Schema(
                Schema\Definition::integer('id', $nullable = true),
                Schema\Definition::string('name', $nullable = true)
            )
        );

        $this->assertSame(
            $notEmptySchema,
            $schema
        );
    }

    public function test_merge_schema() : void
    {
        $schema = (new Schema(
            Schema\Definition::integer('id', $nullable = true),
            Schema\Definition::string('name', $nullable = true)
        ))->merge(
            new Schema(
                Schema\Definition::null('test'),
            )
        );

        $this->assertEquals(
            new Schema(
                Schema\Definition::integer('id', $nullable = true),
                Schema\Definition::string('name', $nullable = true),
                Schema\Definition::null('test'),
            ),
            $schema
        );
    }

    public function test_merge_with_empty_schema() : void
    {
        $schema = ($notEmptySchema = new Schema(
            Schema\Definition::integer('id', $nullable = true),
            Schema\Definition::string('name', $nullable = true)
        ))->merge(
            new Schema()
        );

        $this->assertEquals(
            $notEmptySchema,
            $schema
        );
    }

    public function test_removing_elements_from_schema() : void
    {
        $schema = new Schema(
            Schema\Definition::integer('id'),
            Schema\Definition::string('name'),
        );

        $this->assertEquals(
            new Schema(
                Schema\Definition::integer('id'),
            ),
            $schema->without('name', 'tags')
        );
    }
}
