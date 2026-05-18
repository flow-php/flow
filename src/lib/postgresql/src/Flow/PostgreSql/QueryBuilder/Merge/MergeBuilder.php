<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Merge;

use Flow\PostgreSql\Protobuf\AST\Alias;
use Flow\PostgreSql\Protobuf\AST\MergeStmt;
use Flow\PostgreSql\Protobuf\AST\MergeWhenClause;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\RangeSubselect;
use Flow\PostgreSql\Protobuf\AST\RangeVar;
use Flow\PostgreSql\Protobuf\AST\ResTarget;
use Flow\PostgreSql\QueryBuilder\AstToSql;
use Flow\PostgreSql\QueryBuilder\Clause\WithClause;
use Flow\PostgreSql\QueryBuilder\Condition\Condition;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidExpressionException;
use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;
use Flow\PostgreSql\QueryBuilder\Select\SelectFinalStep;

/**
 * Builder for MERGE statements.
 */
final readonly class MergeBuilder implements MergeIntoStep, MergeOnStep, MergeUsingStep, MergeWhenStep
{
    use AstToSql;

    /**
     * @param list<MergeWhenClauseData> $whenClauses
     */
    private function __construct(
        private ?WithClause $with = null,
        private ?string $table = null,
        private ?string $schema = null,
        private ?string $tableAlias = null,
        private ?string $sourceTable = null,
        private ?string $sourceSchema = null,
        private ?SelectFinalStep $sourceSelect = null,
        private ?string $sourceAlias = null,
        private ?Condition $joinCondition = null,
        private array $whenClauses = [],
    ) {}

    public static function create(): MergeIntoStep
    {
        return new self();
    }

    public static function with(WithClause $with): MergeIntoStep
    {
        return new self(with: $with);
    }

    public function addWhenClause(MergeWhenClauseData $clause): MergeWhenStep
    {
        return new self(
            $this->with,
            $this->table,
            $this->schema,
            $this->tableAlias,
            $this->sourceTable,
            $this->sourceSchema,
            $this->sourceSelect,
            $this->sourceAlias,
            $this->joinCondition,
            [...$this->whenClauses, $clause],
        );
    }

    public function into(string $table, ?string $alias = null): MergeUsingStep
    {
        $identifier = QualifiedIdentifier::parse($table);

        return new self(
            $this->with,
            $identifier->name(),
            $identifier->schema(),
            $alias,
            $this->sourceTable,
            $this->sourceSchema,
            $this->sourceSelect,
            $this->sourceAlias,
            $this->joinCondition,
            $this->whenClauses,
        );
    }

    public function on(Condition $condition): MergeWhenStep
    {
        return new self(
            $this->with,
            $this->table,
            $this->schema,
            $this->tableAlias,
            $this->sourceTable,
            $this->sourceSchema,
            $this->sourceSelect,
            $this->sourceAlias,
            $condition,
            $this->whenClauses,
        );
    }

    public function toAst(): MergeStmt
    {
        if ($this->table === null || $this->table === '') {
            throw InvalidExpressionException::invalidValue('table', 'null or empty');
        }

        if ($this->sourceTable === null && $this->sourceSelect === null) {
            throw InvalidExpressionException::invalidValue('source', 'null');
        }

        $sourceAlias = $this->sourceAlias;

        if ($sourceAlias === null || $sourceAlias === '') {
            throw InvalidExpressionException::invalidValue('sourceAlias', 'null or empty');
        }

        $joinCondition = $this->joinCondition;

        if ($joinCondition === null) {
            throw InvalidExpressionException::invalidValue('joinCondition', 'null');
        }

        if ($this->whenClauses === []) {
            throw InvalidExpressionException::emptyArray('whenClauses');
        }

        $mergeStmt = new MergeStmt();

        $rangeVar = new RangeVar([
            'relname' => $this->table,
            'inh' => true,
        ]);

        if ($this->schema !== null) {
            $rangeVar->setSchemaname($this->schema);
        }

        if ($this->tableAlias !== null) {
            $alias = new Alias();
            $alias->setAliasname($this->tableAlias);
            $rangeVar->setAlias($alias);
        }

        $mergeStmt->setRelation($rangeVar);

        $sourceNode = new Node();

        if ($this->sourceSelect !== null) {
            $rangeSubselect = new RangeSubselect();
            $selectNode = new Node();
            $selectNode->setSelectStmt($this->sourceSelect->toAst());
            $rangeSubselect->setSubquery($selectNode);
            $alias = new Alias();
            $alias->setAliasname($sourceAlias);
            $rangeSubselect->setAlias($alias);
            $sourceNode->setRangeSubselect($rangeSubselect);
        } else {
            $sourceRangeVar = new RangeVar([
                'relname' => $this->sourceTable,
                'inh' => true,
            ]);

            if ($this->sourceSchema !== null) {
                $sourceRangeVar->setSchemaname($this->sourceSchema);
            }

            $alias = new Alias();
            $alias->setAliasname($sourceAlias);
            $sourceRangeVar->setAlias($alias);
            $sourceNode->setRangeVar($sourceRangeVar);
        }

        $mergeStmt->setSourceRelation($sourceNode);

        $mergeStmt->setJoinCondition($joinCondition->toAst());

        $whenClauseNodes = [];

        foreach ($this->whenClauses as $clause) {
            $whenClauseNodes[] = $this->buildWhenClauseNode($clause);
        }

        $mergeStmt->setMergeWhenClauses($whenClauseNodes);

        if ($this->with !== null) {
            $withNode = $this->with->toAst();
            $withClause = $withNode->getWithClause();

            if ($withClause !== null) {
                $mergeStmt->setWithClause($withClause);
            }
        }

        return $mergeStmt;
    }

    public function using(string|SelectFinalStep $source, string $alias): MergeOnStep
    {
        if ($source instanceof SelectFinalStep) {
            return new self(
                $this->with,
                $this->table,
                $this->schema,
                $this->tableAlias,
                null,
                null,
                $source,
                $alias,
                $this->joinCondition,
                $this->whenClauses,
            );
        }

        $identifier = QualifiedIdentifier::parse($source);

        return new self(
            $this->with,
            $this->table,
            $this->schema,
            $this->tableAlias,
            $identifier->name(),
            $identifier->schema(),
            null,
            $alias,
            $this->joinCondition,
            $this->whenClauses,
        );
    }

    public function whenMatched(): MergeWhenMatched
    {
        return MergeWhenMatched::create($this, MergeMatchKind::MATCHED);
    }

    public function whenMatchedAnd(Condition $condition): MergeWhenMatched
    {
        return MergeWhenMatched::create($this, MergeMatchKind::MATCHED, $condition);
    }

    public function whenNotMatched(): MergeWhenNotMatched
    {
        return MergeWhenNotMatched::create($this);
    }

    public function whenNotMatchedAnd(Condition $condition): MergeWhenNotMatched
    {
        return MergeWhenNotMatched::create($this, $condition);
    }

    public function whenNotMatchedBySource(): MergeWhenMatched
    {
        return MergeWhenMatched::create($this, MergeMatchKind::NOT_MATCHED_BY_SOURCE);
    }

    public function whenNotMatchedBySourceAnd(Condition $condition): MergeWhenMatched
    {
        return MergeWhenMatched::create($this, MergeMatchKind::NOT_MATCHED_BY_SOURCE, $condition);
    }

    private function buildWhenClauseNode(MergeWhenClauseData $clause): Node
    {
        $whenClause = new MergeWhenClause();

        $whenClause->setMatchKind($clause->matchKind->value);
        $whenClause->setCommandType($clause->actionType->value);

        if ($clause->condition !== null) {
            $whenClause->setCondition($clause->condition->toAst());
        }

        if ($clause->actionType === MergeActionType::UPDATE && $clause->assignments !== []) {
            $targetList = [];

            foreach ($clause->assignments as $column => $value) {
                $resTarget = new ResTarget([
                    'name' => $column,
                    'val' => $value->toAst(),
                ]);

                $node = new Node();
                $node->setResTarget($resTarget);

                $targetList[] = $node;
            }

            $whenClause->setTargetList($targetList);
        }

        if ($clause->actionType === MergeActionType::INSERT) {
            if ($clause->insertColumns !== []) {
                $targetList = [];

                foreach ($clause->insertColumns as $column) {
                    $resTarget = new ResTarget();
                    $resTarget->setName($column);

                    $node = new Node();
                    $node->setResTarget($resTarget);

                    $targetList[] = $node;
                }

                $whenClause->setTargetList($targetList);
            }

            if ($clause->insertValues !== []) {
                $valueNodes = [];

                foreach ($clause->insertValues as $value) {
                    $valueNodes[] = $value->toAst();
                }

                $whenClause->setValues($valueNodes);
            }
        }

        $node = new Node();
        $node->setMergeWhenClause($whenClause);

        return $node;
    }
}
