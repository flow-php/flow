<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Merge;

use Flow\PgQuery\QueryBuilder\Condition\Condition;
use Flow\PgQuery\QueryBuilder\Expression\Expression;

/**
 * Internal data structure for WHEN clauses.
 */
final readonly class MergeWhenClauseData
{
    /**
     * @param array<string, Expression> $assignments UPDATE SET assignments
     * @param list<string> $insertColumns INSERT column names
     * @param list<Expression> $insertValues INSERT values
     */
    public function __construct(
        public MergeMatchKind $matchKind,
        public MergeActionType $actionType,
        public ?Condition $condition = null,
        public array $assignments = [],
        public array $insertColumns = [],
        public array $insertValues = [],
    ) {
    }
}
