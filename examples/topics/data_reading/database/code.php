<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\Doctrine\from_dbal_limit_offset;
use function Flow\ETL\DSL\{data_frame, to_stream};
use Doctrine\DBAL\DriverManager;
use Flow\ETL\Adapter\Doctrine\{Order, OrderBy};

require __DIR__ . '/../../../autoload.php';

$connection = DriverManager::getConnection([
    'path' => __DIR__ . '/input/orders.db',
    'driver' => 'pdo_sqlite',
]);

data_frame()
    ->read(
        from_dbal_limit_offset(
            $connection,
            'orders',
            new OrderBy('created_at', Order::DESC),
        )
    )
    ->collect()
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
