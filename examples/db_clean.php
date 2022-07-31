#!/usr/bin/env php
<?php

use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;

require __DIR__ . '/../vendor/autoload.php';

$dbConnectionString = 'postgresql://postgres:postgres@127.0.0.1:5432/postgres?serverVersion=11%26charset=utf8';
$dbConnection = DriverManager::getConnection($dbConnectionParams = ['url' => $dbConnectionString]);

foreach ($dbConnection->createSchemaManager()->listTables() as $table) {
    $dbConnection->createSchemaManager()->dropTable($table->getName());
}

$dbConnection->createSchemaManager()->createTable(
    (new Table(
        $tableName = 'flow_dataset_table',
        [
            new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
            new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            new Column('phone', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            new Column('country_code', Type::getType(Types::STRING), ['notnull' => true, 'length' => 2]),
            new Column('t_shirt_color', Type::getType(Types::STRING), ['notnull' => true, 'length' => 64]),
        ],
    ))
    ->setPrimaryKey(['id'])
    ->addUniqueConstraint(['id'], 'flow_dataset_table_uniq')
);

return $dbConnection;
