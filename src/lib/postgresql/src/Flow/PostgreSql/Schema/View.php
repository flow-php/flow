<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use function Flow\PostgreSql\DSL\{create, parsed_select};

use Flow\PostgreSql\QueryBuilder\SqlQuery;

final readonly class View
{
    public function __construct(
        public string $name,
        public string $definition,
        public bool $isUpdatable = false,
    ) {
    }

    public function toSql() : SqlQuery
    {
        return create()->view($this->name)->as(parsed_select($this->definition));
    }
}
