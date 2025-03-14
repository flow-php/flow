<?php

declare(strict_types=1);

use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Tools\DsnParser;

return DriverManager::getConnection(
    (new DsnParser(['postgresql' => 'pdo_pgsql']))
        ->parse(\getenv('PGSQL_DATABASE_URL') ?: '')
);
