<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Extractors;

use Flow\PostgreSql\AST\Visitors\SelectStmtDepthCollector;
use Flow\PostgreSql\ParsedQuery;

final readonly class QueryDepth
{
    public function __construct(
        private ParsedQuery $query,
    ) {}

    public function depth(): int
    {
        $collector = new SelectStmtDepthCollector();
        $this->query->traverse($collector);

        return $collector->getMaxDepth();
    }
}
