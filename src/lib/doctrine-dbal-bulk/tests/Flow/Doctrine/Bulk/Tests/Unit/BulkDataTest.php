<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Unit;

use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Types\{Type, Types};
use Flow\Doctrine\Bulk\{BulkData, Columns, TableDefinition};
use PHPUnit\Framework\TestCase;

final class BulkDataTest extends TestCase
{
    public function test_prevents_creating_bulk_data_for_different_rows() : void
    {
        $this->expectExceptionMessage('Each row must be have the same keys in the same order');

        new BulkData([
            [
                'date' => 'today',
                'title' => 'Title One',
                'description' => 'Description One',
                'quantity' => 101,
            ],
            [
                'title' => 'Title One',
                'date' => 'today',
                'quantity' => 101,
                'description' => 'Description One',
            ],
        ]);
    }

    public function test_prevents_creating_bulk_data_from_invalid_rows() : void
    {
        $this->expectExceptionMessage('Each row must be an array');

        new BulkData([1, 2, 3]);
    }

    public function test_prevents_creating_bulk_data_from_invalid_rows_when_first_row_is_an_array() : void
    {
        $this->expectExceptionMessage('Each row must be an array');

        new BulkData([
            [
                'date' => 'today',
                'title' => 'Title One',
                'description' => 'Description One',
                'quantity' => 101,
            ],
            'not-an-array',
        ]);
    }

    public function test_prevents_creating_empty_bulk_data() : void
    {
        $this->expectExceptionMessage('Bulk data cannot be empty');

        new BulkData([]);
    }

    public function test_returns_all_sql_parameters_as_one_dimensional_array_with_placeholders_as_keys() : void
    {
        $bulkData = new BulkData([
            [
                'date' => 'today',
                'title' => 'Title One',
                'description' => 'Description One',
                'quantity' => 101,
                'errors' => '[]',
            ],
            [
                'date' => 'today',
                'title' => 'Title Two',
                'description' => 'Description Two',
                'quantity' => 102,
                'errors' => '[]',
            ],
        ]);

        $registry = Type::getTypeRegistry();

        self::assertEquals(
            [
                'date_0' => 'today',
                'title_0' => 'Title One',
                'description_0' => 'Description One',
                'quantity_0' => 101,
                'errors_0' => [],
                'date_1' => 'today',
                'title_1' => 'Title Two',
                'description_1' => 'Description Two',
                'quantity_1' => 102,
                'errors_1' => [],
            ],
            $bulkData->toSqlParameters(
                new TableDefinition(
                    'test',
                    new Column('date', $registry->get(Types::STRING)),
                    new Column('title', $registry->get(Types::STRING)),
                    new Column('description', $registry->get(Types::STRING)),
                    new Column('quantity', $registry->get(Types::INTEGER)),
                    new Column('errors', $registry->get(Types::JSON)),
                )
            )
        );
    }

    public function test_returns_columns() : void
    {
        $bulkData = new BulkData([
            [
                'date' => 'today',
                'title' => 'Title One',
                'description' => 'Description One',
                'quantity' => 101,
            ],
            [
                'date' => 'today',
                'title' => 'Title Two',
                'description' => 'Description Two',
                'quantity' => 102,
            ],
        ]);

        self::assertEquals(
            new Columns('date', 'title', 'description', 'quantity'),
            $bulkData->columns()
        );
    }

    public function test_returns_rows_with_numeric_indexes_even_when_provided_no_sorted() : void
    {
        $bulkData = new BulkData([
            5 => [
                'date' => 'today',
                'title' => 'Title One',
                'description' => 'Description One',
                'quantity' => 101,
            ],
            10 => [
                'date' => 'today',
                'title' => 'Title Two',
                'description' => 'Description Two',
                'quantity' => 102,
            ],
        ]);

        self::assertEquals(
            [
                0 => [
                    'date' => 'today',
                    'title' => 'Title One',
                    'description' => 'Description One',
                    'quantity' => 101,
                ],
                1 => [
                    'date' => 'today',
                    'title' => 'Title Two',
                    'description' => 'Description Two',
                    'quantity' => 102,
                ],
            ],
            $bulkData->rows()
        );
    }

    public function test_returns_sql_rows() : void
    {
        $bulkData = new BulkData([
            5 => [
                'date' => 'today',
                'title' => 'Title One',
                'description' => 'Description One',
                'quantity' => 101,
            ],
            10 => [
                'date' => 'today',
                'title' => 'Title Two',
                'description' => 'Description Two',
                'quantity' => 102,
            ],
        ]);

        self::assertEquals(
            [
                0 => [
                    'date_0' => 'today',
                    'title_0' => 'Title One',
                    'description_0' => 'Description One',
                    'quantity_0' => 101,
                ],
                1 => [
                    'date_1' => 'today',
                    'title_1' => 'Title Two',
                    'description_1' => 'Description Two',
                    'quantity_1' => 102,
                ],
            ],
            $bulkData->sqlRows()
        );
    }

    public function test_transforms_data_to_sql_values_placeholders() : void
    {
        $bulkData = new BulkData([
            [
                'date' => 'today',
                'title' => 'Title One',
                'description' => 'Description One',
                'quantity' => 101,
            ],
            [
                'date' => 'today',
                'title' => 'Title Two',
                'description' => 'Description Two',
                'quantity' => 102,
            ],
        ]);

        self::assertEquals(
            '(:date_0,:title_0,:description_0,:quantity_0),(:date_1,:title_1,:description_1,:quantity_1)',
            $bulkData->toSqlPlaceholders()
        );
    }
}
