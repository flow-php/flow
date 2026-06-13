<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Debug;

final class QueryLog
{
    /**
     * @var list<RecordedQuery>
     */
    private array $queries = [];

    public function add(RecordedQuery $query): void
    {
        $this->queries[] = $query;
    }

    /**
     * @return list<RecordedQuery>
     */
    public function queries(): array
    {
        return $this->queries;
    }

    public function reset(): void
    {
        $this->queries = [];
    }
}
