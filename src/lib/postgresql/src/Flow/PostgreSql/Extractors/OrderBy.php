<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Extractors;

use Flow\PostgreSql\AST\Nodes\OrderByItem;
use Flow\PostgreSql\AST\Visitors\SortByCollector;
use Flow\PostgreSql\ParsedQuery;

final readonly class OrderBy
{
    public function __construct(private ParsedQuery $query)
    {
    }

    /**
     * @return array<OrderByItem>
     */
    public function all() : array
    {
        $collector = new SortByCollector();
        $this->query->traverse($collector);

        return \array_map(
            static fn ($sortBy) => new OrderByItem($sortBy),
            $collector->getSortByClauses()
        );
    }

    public function hasOrderBy() : bool
    {
        $collector = new SortByCollector();
        $this->query->traverse($collector);

        return $collector->hasSortBy();
    }
}
