<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Unit;

use Flow\Doctrine\Bulk\Columns;
use PHPUnit\Framework\TestCase;

final class ColumnsTest extends TestCase
{
    public function test_adds_index_on_the_end_of_all_columns() : void
    {
        $columns = new Columns('date', 'title', 'description', 'quantity');

        $this->assertEquals(
            new Columns('date_7', 'title_7', 'description_7', 'quantity_7'),
            $columns->suffix('_7')
        );
    }

    public function test_prevents_creating_duplicated_columns() : void
    {
        $this->expectExceptionMessage('All columns must be unique');

        new Columns('date', 'title', 'date');
    }

    public function test_prevents_creating_empty_columns() : void
    {
        $this->expectExceptionMessage('Columns cannot be empty');

        new Columns();
    }

    public function test_that_collection_contains_columns() : void
    {
        $columns = new Columns('date', 'title', 'description', 'quantity');

        $this->assertTrue($columns->has('date'));
        $this->assertTrue($columns->has('date', 'title'));
        $this->assertFalse($columns->has('row'));
    }

    public function test_that_collection_not_contains_columns() : void
    {
        $columns = new Columns('date', 'title', 'description', 'quantity');
        $this->assertEquals(
            new Columns('title', 'description', 'quantity'),
            $columns->without('date')
        );

        $columns = new Columns('date', 'title', 'description', 'quantity');
        $this->assertEquals(
            new Columns('title', 'quantity'),
            $columns->without('date', 'description')
        );
    }

    public function test_transforms_all_columns_to_placeholders() : void
    {
        $columns = new Columns('date', 'title', 'description', 'quantity');

        $this->assertEquals(
            new Columns(':date', ':title', ':description', ':quantity'),
            $columns->prefix(':')
        );
    }
}
