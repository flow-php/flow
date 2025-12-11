<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Merge;

use Flow\PostgreSql\QueryBuilder\Condition\Condition;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;

/**
 * Builder for WHEN MATCHED clause in MERGE statement.
 * Supports UPDATE, DELETE, and DO NOTHING actions.
 */
final readonly class MergeWhenMatched
{
    private function __construct(
        private MergeBuilder $builder,
        private MergeMatchKind $matchKind,
        private ?Condition $condition,
    ) {
    }

    public static function create(
        MergeBuilder $builder,
        MergeMatchKind $matchKind,
        ?Condition $condition = null,
    ) : self {
        return new self($builder, $matchKind, $condition);
    }

    public function thenDelete() : MergeWhenStep
    {
        return $this->builder->addWhenClause(
            new MergeWhenClauseData(
                $this->matchKind,
                MergeActionType::DELETE,
                $this->condition,
            )
        );
    }

    public function thenDoNothing() : MergeWhenStep
    {
        return $this->builder->addWhenClause(
            new MergeWhenClauseData(
                $this->matchKind,
                MergeActionType::DO_NOTHING,
                $this->condition,
            )
        );
    }

    /**
     * @param array<string, Expression> $assignments Column => value pairs for SET clause
     */
    public function thenUpdate(array $assignments) : MergeWhenStep
    {
        return $this->builder->addWhenClause(
            new MergeWhenClauseData(
                $this->matchKind,
                MergeActionType::UPDATE,
                $this->condition,
                $assignments,
            )
        );
    }
}
