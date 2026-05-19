<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Extractors;

use Flow\PostgreSql\AST\Visitors\SortByCollector;
use Flow\PostgreSql\ParsedQuery;
use Flow\PostgreSql\QueryBuilder\Clause\OrderBy as OrderByClause;

use function array_map;

final readonly class OrderBy
{
    public function __construct(
        private ParsedQuery $query,
    ) {}

    /**
     * @return array<OrderByClause>
     */
    public function all(): array
    {
        $collector = new SortByCollector();
        $this->query->traverse($collector);

        return array_map(static fn($sortBy) => OrderByClause::fromAst($sortBy), $collector->getSortByClauses());
    }

    public function hasOrderBy(): bool
    {
        $collector = new SortByCollector();
        $this->query->traverse($collector);

        return $collector->hasSortBy();
    }
}
