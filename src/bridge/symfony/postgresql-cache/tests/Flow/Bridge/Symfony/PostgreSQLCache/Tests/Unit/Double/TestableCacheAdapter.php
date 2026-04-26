<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLCache\Tests\Unit\Double;

use Flow\Bridge\Symfony\PostgreSQLCache\FlowPostgreSqlCacheAdapter;

final class TestableCacheAdapter extends FlowPostgreSqlCacheAdapter
{
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
