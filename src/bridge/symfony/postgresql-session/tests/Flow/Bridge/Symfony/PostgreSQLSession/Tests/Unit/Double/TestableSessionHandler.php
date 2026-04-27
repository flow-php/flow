<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLSession\Tests\Unit\Double;

use Flow\Bridge\Symfony\PostgreSQLSession\FlowPostgreSqlSessionHandler;
use Flow\PostgreSql\Client\Client;

final class TestableSessionHandler extends FlowPostgreSqlSessionHandler
{
    /**
     * @param array{
     *     db_table?: string,
     *     db_schema?: string,
     *     db_id_col?: string,
     *     db_data_col?: string,
     *     db_lifetime_col?: string,
     *     db_time_col?: string,
     *     lock_mode?: int,
     *     ttl?: null|int,
     * } $options
     */
    public function __construct(
        Client $client,
        array $options = [],
    ) {
        parent::__construct($client, $options);
    }

    public function exposedDoDestroy(string $sessionId) : bool
    {
        return $this->doDestroy($sessionId);
    }

    public function exposedDoRead(string $sessionId) : string
    {
        return $this->doRead($sessionId);
    }

    public function exposedDoWrite(string $sessionId, string $data) : bool
    {
        return $this->doWrite($sessionId, $data);
    }
}
