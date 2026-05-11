<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Extractors;

use Flow\PostgreSql\AST\Nodes\Table;
use Flow\PostgreSql\AST\Visitors\RangeVarCollector;
use Flow\PostgreSql\ParsedQuery;

final readonly class Tables
{
    public function __construct(
        private ParsedQuery $query,
    ) {}

    /**
     * @return array<Table>
     */
    public function all(): array
    {
        $collector = new RangeVarCollector();
        $this->query->traverse($collector);

        return \array_values(\array_filter(
            \array_map(static fn($ref) => new Table($ref), $collector->getRangeVars()),
            static fn($table) => $table->name() !== '',
        ));
    }
}
