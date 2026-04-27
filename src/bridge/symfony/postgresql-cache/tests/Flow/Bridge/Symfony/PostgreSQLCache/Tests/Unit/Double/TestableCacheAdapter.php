<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLCache\Tests\Unit\Double;

use Flow\Bridge\Symfony\PostgreSQLCache\FlowPostgreSqlCacheAdapter;
use Flow\PostgreSql\Client\Client;
use Symfony\Component\Cache\Marshaller\MarshallerInterface;

final class TestableCacheAdapter extends FlowPostgreSqlCacheAdapter
{
    /**
     * @param array{db_table?: string, db_schema?: string, db_id_col?: string, db_data_col?: string, db_lifetime_col?: string, db_time_col?: string} $options
     */
    public function __construct(
        Client $client,
        string $namespace = '',
        int $defaultLifetime = 0,
        array $options = [],
        ?MarshallerInterface $marshaller = null,
    ) {
        parent::__construct(
            $client,
            $namespace,
            $defaultLifetime,
            $options,
            $marshaller,
        );
    }

    public function exposedDoClear(string $namespace) : bool
    {
        return $this->doClear($namespace);
    }

    /**
     * @param array<int, string> $ids
     */
    public function exposedDoDelete(array $ids) : bool
    {
        return $this->doDelete($ids);
    }

    /**
     * @param array<int, string> $ids
     *
     * @return iterable<string, mixed>
     */
    public function exposedDoFetch(array $ids) : iterable
    {
        return $this->doFetch($ids);
    }

    public function exposedDoHave(string $id) : bool
    {
        return $this->doHave($id);
    }

    /**
     * @param array<string, mixed> $values
     *
     * @return array<int, string>|bool
     */
    public function exposedDoSave(array $values, int $lifetime) : array|bool
    {
        return $this->doSave($values, $lifetime);
    }
}
