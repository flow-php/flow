<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Row\Schema;
use PHPUnit\Framework\TestCase;

final class SchemaTest extends TestCase
{
    public function test_row_with_a_missing_entry() : void
    {
        $schema = new Schema(
            Schema\Definition::integer('id'),
            Schema\Definition::string('name'),
        );

        $this->assertFalse(
            $schema->isValid(Row::create(Entry::integer('id', 1), Entry::string('name', 'test'), Entry::boolean('active', true)))
        );
    }

    public function test_row_with_all_entries_valid() : void
    {
        $schema = new Schema(
            Schema\Definition::integer('id'),
            Schema\Definition::string('name'),
            Schema\Definition::boolean('active'),
        );

        $this->assertTrue(
            $schema->isValid(Row::create(Entry::integer('id', 1), Entry::string('name', 'test'), Entry::boolean('active', true)))
        );
    }

    public function test_row_with_an_extra_entry() : void
    {
        $schema = new Schema(
            Schema\Definition::integer('id'),
            Schema\Definition::string('name'),
            Schema\Definition::boolean('active'),
            Schema\Definition::array('tags'),
        );

        $this->assertFalse(
            $schema->isValid(Row::create(Entry::integer('id', 1), Entry::string('name', 'test'), Entry::boolean('active', true)))
        );
    }

    public function test_row_with_single_invalid_entry() : void
    {
        $schema = new Schema(
            Schema\Definition::integer('id'),
            Schema\Definition::boolean('name'),
            Schema\Definition::boolean('active'),
        );

        $this->assertFalse(
            $schema->isValid(Row::create(Entry::integer('id', 1), Entry::string('name', 'test'), Entry::boolean('active', true)))
        );
    }
}
