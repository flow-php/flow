<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Schema;

use Flow\ETL\DSL\Entry;
use Flow\ETL\PHP\Type\Native\ScalarType;
use Flow\ETL\Row;
use Flow\ETL\Row\Schema;
use Flow\ETL\Row\Schema\SelectiveValidator;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class SelectiveValidatorTest extends TestCase
{
    public function test_rows_with_a_missing_entry() : void
    {
        $schema = new Schema(
            Schema\Definition::integer('id', ScalarType::integer64()),
            Schema\Definition::string('name'),
        );

        $this->assertFalse(
            (new SelectiveValidator())->isValid(
                new Rows(Row::create(Entry::integer('id', 1), Entry::boolean('active', true))),
                $schema
            )
        );
    }

    public function test_rows_with_an_extra_entry() : void
    {
        $schema = new Schema(
            Schema\Definition::integer('id', ScalarType::integer64()),
            Schema\Definition::string('name'),
        );

        $this->assertTrue(
            (new SelectiveValidator())->isValid(
                new Rows(Row::create(Entry::integer('id', 1), Entry::string('name', 'test'), Entry::boolean('active', true))),
                $schema
            )
        );
    }

    public function test_rows_with_single_invalid_entry() : void
    {
        $schema = new Schema(
            Schema\Definition::integer('id', ScalarType::integer64()),
            Schema\Definition::boolean('name'),
            Schema\Definition::boolean('active'),
        );

        $this->assertFalse(
            (new SelectiveValidator())->isValid(
                new Rows(Row::create(Entry::integer('id', 1), Entry::string('name', 'test'), Entry::boolean('active', true))),
                $schema
            )
        );
    }

    public function test_rows_with_single_invalid_row() : void
    {
        $schema = new Schema(
            Schema\Definition::string('name'),
            Schema\Definition::boolean('active'),
        );

        $this->assertFalse(
            (new SelectiveValidator())->isValid(
                new Rows(
                    Row::create(Entry::integer('id', 1), Entry::string('name', 'test'), Entry::boolean('active', true)),
                    Row::create(Entry::integer('id', 1), Entry::boolean('active', true))
                ),
                $schema
            )
        );
    }
}
