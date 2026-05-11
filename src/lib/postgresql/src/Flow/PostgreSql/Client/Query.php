<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client;

use Flow\PostgreSql\QueryBuilder\Sql;

final readonly class Query
{
    /**
     * @param list<mixed> $parameters
     */
    public function __construct(
        private Sql|string $sql,
        private array $parameters = [],
    ) {}

    /**
     * @return list<mixed>
     */
    public function parameters(): array
    {
        return $this->parameters;
    }

    public function sql(): Sql|string
    {
        return $this->sql;
    }
}
