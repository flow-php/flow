<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Mother;

use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Client\Context as ClientContext;
use Flow\PostgreSql\Client\Query;
use Flow\PostgreSql\Client\RowMapper\Context;

final class MapperContextMother
{
    public static function any(): Context
    {
        return new Context(new Query('SELECT 1'), new StubClient(), new ClientContext());
    }

    public static function with(
        ?Query $query = null,
        ?Client $client = null,
        ?ClientContext $clientContext = null,
    ): Context {
        return new Context(
            $query ?? new Query('SELECT 1'),
            $client ?? new StubClient(),
            $clientContext ?? new ClientContext(),
        );
    }

    /**
     * @param array<string, mixed> $data
     */
    public static function withData(array $data): Context
    {
        return new Context(new Query('SELECT 1'), new StubClient(), new ClientContext(data: $data));
    }
}
