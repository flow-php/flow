<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Merge;

use Flow\PgQuery\QueryBuilder\Condition\Condition;
use Flow\PgQuery\QueryBuilder\Expression\Expression;

/**
 * Builder for WHEN NOT MATCHED clause in MERGE statement.
 * Supports INSERT and DO NOTHING actions.
 */
final readonly class MergeWhenNotMatched
{
    private function __construct(
        private MergeBuilder $builder,
        private ?Condition $condition,
    ) {
    }

    public static function create(MergeBuilder $builder, ?Condition $condition = null) : self
    {
        return new self($builder, $condition);
    }

    public function thenDoNothing() : MergeWhenStep
    {
        return $this->builder->addWhenClause(
            new MergeWhenClauseData(
                MergeMatchKind::NOT_MATCHED_BY_TARGET,
                MergeActionType::DO_NOTHING,
                $this->condition,
            )
        );
    }

    /**
     * Insert specific columns with values.
     *
     * @param list<string> $columns Column names
     * @param list<Expression> $values Values to insert
     */
    public function thenInsert(array $columns, array $values) : MergeWhenStep
    {
        return $this->builder->addWhenClause(
            new MergeWhenClauseData(
                MergeMatchKind::NOT_MATCHED_BY_TARGET,
                MergeActionType::INSERT,
                $this->condition,
                [],
                $columns,
                $values,
            )
        );
    }

    /**
     * Insert with column => value pairs.
     *
     * @param array<string, Expression> $columnValuePairs
     */
    public function thenInsertValues(array $columnValuePairs) : MergeWhenStep
    {
        $columns = \array_keys($columnValuePairs);
        $values = \array_values($columnValuePairs);

        return $this->builder->addWhenClause(
            new MergeWhenClauseData(
                MergeMatchKind::NOT_MATCHED_BY_TARGET,
                MergeActionType::INSERT,
                $this->condition,
                [],
                $columns,
                $values,
            )
        );
    }
}
