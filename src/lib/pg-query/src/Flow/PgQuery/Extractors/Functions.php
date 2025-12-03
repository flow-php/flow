<?php

declare(strict_types=1);

namespace Flow\PgQuery\Extractors;

use Flow\PgQuery\AST\Nodes\FunctionCall;
use Flow\PgQuery\AST\Visitors\FuncCallCollector;
use Flow\PgQuery\ParsedQuery;

final readonly class Functions
{
    public function __construct(private ParsedQuery $query)
    {
    }

    /**
     * @return array<FunctionCall>
     */
    public function all() : array
    {
        $collector = new FuncCallCollector();
        $this->query->traverse($collector);

        return \array_values(\array_filter(
            \array_map(static fn ($ref) => new FunctionCall($ref), $collector->getFuncCalls()),
            static fn ($func) => $func->name() !== null
        ));
    }
}
