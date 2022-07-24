<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Unit;

use Doctrine\DBAL\Platforms\PostgreSQL100Platform;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Types\Type;
use Flow\Doctrine\Bulk\BulkData;
use Flow\Doctrine\Bulk\Exception\RuntimeException;
use Flow\Doctrine\Bulk\TableDefinition;
use PHPUnit\Framework\TestCase;

final class TableDefinitionTest extends TestCase
{
    public function test_getting_types_from_bulk_data() : void
    {
        $data = new BulkData(
            [
                ['id' => 1, 'name' => 'test', 'updated_at' => new \DateTimeImmutable()],
                ['id' => 2, 'name' => 'test', 'updated_at' => new \DateTimeImmutable()],
            ]
        );

        $table = new TableDefinition(
            'test',
            new Column('id', Type::getType('integer')),
            new Column('name', Type::getType('string'), ['length' => 256]),
            new Column('updated_at', Type::getType('datetime_immutable')),
        );

        $this->assertSame(
            [
                'id_0' => 'integer',
                'id_1' => 'integer',
                'name_0' => 'string',
                'name_1' => 'string',
                'updated_at_0' => 'datetime_immutable',
                'updated_at_1' => 'datetime_immutable',
            ],
            $table->dbalTypes($data)
        );
    }

    public function test_getting_sql_typed_placeholders() : void
    {
        $data = new BulkData(
            [
                ['id' => 1, 'name' => 'test', 'updated_at' => new \DateTimeImmutable()],
                ['id' => 2, 'name' => 'test', 'updated_at' => new \DateTimeImmutable()],
            ]
        );

        $table = new TableDefinition(
            'test',
            new Column('id', Type::getType('integer')),
            new Column('name', Type::getType('string')),
            new Column('updated_at', Type::getType('datetime_immutable')),
        );

        $this->assertSame(
            '(CAST(:id_0 as INT),CAST(:name_0 as VARCHAR(255)),CAST(:updated_at_0 as TIMESTAMP(0) WITHOUT TIME ZONE)),(CAST(:id_1 as INT),CAST(:name_1 as VARCHAR(255)),CAST(:updated_at_1 as TIMESTAMP(0) WITHOUT TIME ZONE))',
            $table->toSqlCastedPlaceholders($data, new PostgreSQL100Platform())
        );
    }

    public function test_getting_types_from_bulk_data_with_column_not_available_in_db_schema() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Column with name updated_at, not found in table: some_table_name');

        $data = new BulkData(
            [
                ['id' => 1, 'name' => 'test', 'updated_at' => new \DateTimeImmutable()],
                ['id' => 2, 'name' => 'test', 'updated_at' => new \DateTimeImmutable()],
            ]
        );

        $table = new TableDefinition(
            'some_table_name',
            new Column('id', Type::getType('integer')),
            new Column('name', Type::getType('string')),
        );

        $table->dbalTypes($data);
    }
}
