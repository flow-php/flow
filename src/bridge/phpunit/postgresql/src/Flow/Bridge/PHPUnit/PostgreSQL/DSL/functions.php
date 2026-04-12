<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\PostgreSQL\DSL;

use Flow\Bridge\PHPUnit\PostgreSQL\StaticClient;
use Flow\PostgreSql\Client\{Client, ConnectionParameters};
use Flow\PostgreSql\Client\Types\ValueConverters;

function static_pgsql_client(ConnectionParameters $params, ?ValueConverters $converters = null) : Client
{
    return StaticClient::connect($params, $converters);
}
