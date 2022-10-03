#!/usr/bin/env php
<?php

use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Flow\ETL\Adapter\Doctrine\DbalLoader;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;

require __DIR__ . '/../../bootstrap.php';

$dbConnectionString = 'postgresql://postgres:postgres@127.0.0.1:5432/postgres?serverVersion=11%26charset=utf8';
$dbConnection = DriverManager::getConnection($dbConnectionParams = ['url' => $dbConnectionString]);

$dbConnection->createSchemaManager()->createTable(
    (new Table(
        $tableName = 'source_dataset_table',
        [
            new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
            new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            new Column('last_name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            new Column('phone', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            new Column('country_code', Type::getType(Types::STRING), ['notnull' => true, 'length' => 2]),
            new Column('t_shirt_color', Type::getType(Types::STRING), ['notnull' => true, 'length' => 64]),
        ],
    ))
    ->setPrimaryKey(['id'])
    ->addUniqueConstraint(['id'], 'source_dataset_table_uniq')
);

(new Flow())
    ->read(CSV::from($path = __FLOW_OUTPUT__ . '/dataset.csv', 10_000))
    ->rows(Transform::array_unpack('row'))
    ->drop('row')
    ->rename('last name', 'last_name')
    ->limit(1_000_000)
    ->load(DbalLoader::fromConnection($dbConnection, 'source_dataset_table', 1000))
    ->run();

return [
    $dbConnection,
    $dbConnection->executeQuery('SELECT COUNT(*) FROM source_dataset_table')->fetchFirstColumn()[0],
];
