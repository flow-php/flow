<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Select;

use Flow\PostgreSql\Protobuf\AST\LimitOption;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ResTarget;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use Flow\PostgreSql\QueryBuilder\AstToSql;
use Flow\PostgreSql\QueryBuilder\Clause\LockingClause;
use Flow\PostgreSql\QueryBuilder\Clause\OrderBy;
use Flow\PostgreSql\QueryBuilder\Clause\WindowDefinition;
use Flow\PostgreSql\QueryBuilder\Clause\WithClause;
use Flow\PostgreSql\QueryBuilder\Condition\Condition;
use Flow\PostgreSql\QueryBuilder\Condition\ConditionBuilder;
use Flow\PostgreSql\QueryBuilder\Condition\ConditionFactory;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidBuilderStateException;
use Flow\PostgreSql\QueryBuilder\Expression\AliasedExpression;
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;
use Flow\PostgreSql\QueryBuilder\Expression\ExpressionFactory;
use Flow\PostgreSql\QueryBuilder\Expression\Literal;
use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;
use Flow\PostgreSql\QueryBuilder\Table\AliasedTable;
use Flow\PostgreSql\QueryBuilder\Table\DerivedTable;
use Flow\PostgreSql\QueryBuilder\Table\JoinedTable;
use Flow\PostgreSql\QueryBuilder\Table\JoinType;
use Flow\PostgreSql\QueryBuilder\Table\Table;
use Flow\PostgreSql\QueryBuilder\Table\TableFunction;
use Flow\PostgreSql\QueryBuilder\Table\TableReference;

use function array_map;
use function array_merge;
use function array_reverse;
use function array_values;
use function count;
use function is_int;
use function is_string;

final readonly class SelectBuilder implements SelectFromStep, SelectJoinStep, SelectSelectStep
{
    use AstToSql;

    /**
     * @param array<Expression> $selectList
     * @param array<Expression> $distinctOn
     * @param array<TableReference> $from
     * @param array<JoinedTable> $joins
     * @param array<Expression> $groupBy
     * @param array<WindowDefinition> $windows
     * @param array<OrderBy> $orderBy
     * @param array<LockingClause> $locks
     */
    private function __construct(
        private ?WithClause $with = null,
        private array $selectList = [],
        private bool $distinct = false,
        private array $distinctOn = [],
        private array $from = [],
        private array $joins = [],
        private ?Condition $where = null,
        private array $groupBy = [],
        private ?Condition $having = null,
        private array $windows = [],
        private ?SetOperation $setOp = null,
        private ?SelectFinalStep $setOpRhs = null,
        private ?SelectFinalStep $setOpLhs = null,
        private array $orderBy = [],
        private ?int $limit = null,
        private ?int $offset = null,
        private array $locks = [],
    ) {}

    public static function create(): self
    {
        return new self();
    }

    public static function fromAst(SelectStmt $selectStmt): static
    {
        $with = null;
        $withClause = $selectStmt->getWithClause();

        if ($withClause !== null) {
            $with = WithClause::fromAst($withClause);
        }

        $larg = $selectStmt->getLarg();

        if ($larg !== null) {
            return self::fromSetOperation($selectStmt, $with);
        }

        $selectList = [];

        foreach ($selectStmt->getTargetList() as $targetNode) {
            $resTarget = $targetNode->getResTarget();

            if ($resTarget !== null && $resTarget->getName() !== '') {
                $selectList[] = AliasedExpression::fromAst($targetNode);
            } else {
                $valNode = $resTarget?->getVal();

                if ($valNode !== null) {
                    $selectList[] = ExpressionFactory::fromAst($valNode);
                }
            }
        }

        $distinct = false;
        $distinctOn = [];
        $distinctClause = $selectStmt->getDistinctClause();

        if (count($distinctClause) > 0) {
            $distinct = true;

            foreach ($distinctClause as $distinctNode) {
                if ($distinctNode->serializeToJsonString() !== '{}') {
                    $distinctOn[] = ExpressionFactory::fromAst($distinctNode);
                }
            }
        }

        $from = [];
        $joins = [];

        foreach ($selectStmt->getFromClause() as $fromNode) {
            if ($fromNode->hasJoinExpr()) {
                $flattenedJoins = self::flattenJoins($fromNode);
                $base = $flattenedJoins['base'];

                if ($base !== null) {
                    $from[] = $base;
                }

                $joins = array_merge($joins, $flattenedJoins['joins']);
            } else {
                $from[] = self::tableReferenceFromNode($fromNode);
            }
        }

        $where = null;
        $whereClause = $selectStmt->getWhereClause();

        if ($whereClause !== null) {
            $where = ConditionFactory::fromAst($whereClause);
        }

        $groupBy = [];

        foreach ($selectStmt->getGroupClause() as $groupNode) {
            $groupBy[] = ExpressionFactory::fromAst($groupNode);
        }

        $having = null;
        $havingClause = $selectStmt->getHavingClause();

        if ($havingClause !== null) {
            $having = ConditionFactory::fromAst($havingClause);
        }

        $windows = [];

        foreach ($selectStmt->getWindowClause() as $windowNode) {
            $windows[] = WindowDefinition::fromAst($windowNode);
        }

        $orderBy = [];

        foreach ($selectStmt->getSortClause() as $sortNode) {
            $sortBy = $sortNode->getSortBy();

            if ($sortBy !== null) {
                $orderBy[] = OrderBy::fromAst($sortBy);
            }
        }

        $limit = null;
        $limitCount = $selectStmt->getLimitCount();

        if ($limitCount !== null) {
            $limitExpr = ExpressionFactory::fromAst($limitCount);

            if ($limitExpr instanceof Literal && $limitExpr->isInt()) {
                $limitValue = $limitExpr->value();
                $limit = is_int($limitValue) ? $limitValue : null;
            }
        }

        $offset = null;
        $limitOffset = $selectStmt->getLimitOffset();

        if ($limitOffset !== null) {
            $offsetExpr = ExpressionFactory::fromAst($limitOffset);

            if ($offsetExpr instanceof Literal && $offsetExpr->isInt()) {
                $offsetValue = $offsetExpr->value();
                $offset = is_int($offsetValue) ? $offsetValue : null;
            }
        }

        $locks = [];

        foreach ($selectStmt->getLockingClause() as $lockNode) {
            $locks[] = LockingClause::fromAst($lockNode);
        }

        return new self(
            with: $with,
            selectList: $selectList,
            distinct: $distinct,
            distinctOn: $distinctOn,
            from: $from,
            joins: $joins,
            where: $where,
            groupBy: $groupBy,
            having: $having,
            windows: $windows,
            orderBy: $orderBy,
            limit: $limit,
            offset: $offset,
            locks: $locks,
        );
    }

    public static function with(WithClause $with): SelectSelectStep
    {
        return new self(with: $with);
    }

    public function crossJoin(string|TableReference $table): SelectJoinStep
    {
        if (is_string($table)) {
            $id = QualifiedIdentifier::parse($table);
            $table = new Table($id->name(), $id->schema());
        }

        $join = new JoinedTable(left: $this->from[count($this->from) - 1], right: $table, joinType: JoinType::CROSS);

        return new self(
            with: $this->with,
            selectList: $this->selectList,
            distinct: $this->distinct,
            distinctOn: $this->distinctOn,
            from: $this->from,
            joins: [...$this->joins, $join],
            where: $this->where,
            groupBy: $this->groupBy,
            having: $this->having,
            windows: $this->windows,
            setOp: $this->setOp,
            setOpRhs: $this->setOpRhs,
            setOpLhs: $this->setOpLhs,
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function except(SelectFinalStep $other): SelectSetOperationStep
    {
        return $this->setOperation(SetOperation::EXCEPT, $other);
    }

    public function exceptAll(SelectFinalStep $other): SelectSetOperationStep
    {
        return $this->setOperation(SetOperation::EXCEPT_ALL, $other);
    }

    public function forKeyShare(string ...$tables): SelectFinalStep
    {
        $lock = LockingClause::forKeyShare(array_values($tables));

        return new self(
            with: $this->with,
            selectList: $this->selectList,
            distinct: $this->distinct,
            distinctOn: $this->distinctOn,
            from: $this->from,
            joins: $this->joins,
            where: $this->where,
            groupBy: $this->groupBy,
            having: $this->having,
            windows: $this->windows,
            setOp: $this->setOp,
            setOpRhs: $this->setOpRhs,
            setOpLhs: $this->setOpLhs,
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: [...$this->locks, $lock],
        );
    }

    public function forNoKeyUpdate(string ...$tables): SelectFinalStep
    {
        $lock = LockingClause::forNoKeyUpdate(array_values($tables));

        return new self(
            with: $this->with,
            selectList: $this->selectList,
            distinct: $this->distinct,
            distinctOn: $this->distinctOn,
            from: $this->from,
            joins: $this->joins,
            where: $this->where,
            groupBy: $this->groupBy,
            having: $this->having,
            windows: $this->windows,
            setOp: $this->setOp,
            setOpRhs: $this->setOpRhs,
            setOpLhs: $this->setOpLhs,
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: [...$this->locks, $lock],
        );
    }

    public function forShare(string ...$tables): SelectFinalStep
    {
        $lock = LockingClause::forShare(array_values($tables));

        return new self(
            with: $this->with,
            selectList: $this->selectList,
            distinct: $this->distinct,
            distinctOn: $this->distinctOn,
            from: $this->from,
            joins: $this->joins,
            where: $this->where,
            groupBy: $this->groupBy,
            having: $this->having,
            windows: $this->windows,
            setOp: $this->setOp,
            setOpRhs: $this->setOpRhs,
            setOpLhs: $this->setOpLhs,
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: [...$this->locks, $lock],
        );
    }

    public function forUpdate(string ...$tables): SelectFinalStep
    {
        $lock = LockingClause::forUpdate(array_values($tables));

        return new self(
            with: $this->with,
            selectList: $this->selectList,
            distinct: $this->distinct,
            distinctOn: $this->distinctOn,
            from: $this->from,
            joins: $this->joins,
            where: $this->where,
            groupBy: $this->groupBy,
            having: $this->having,
            windows: $this->windows,
            setOp: $this->setOp,
            setOpRhs: $this->setOpRhs,
            setOpLhs: $this->setOpLhs,
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: [...$this->locks, $lock],
        );
    }

    public function forUpdateSkipLocked(string ...$tables): SelectFinalStep
    {
        $lock = LockingClause::forUpdate(array_values($tables))->skipLocked();

        return new self(
            with: $this->with,
            selectList: $this->selectList,
            distinct: $this->distinct,
            distinctOn: $this->distinctOn,
            from: $this->from,
            joins: $this->joins,
            where: $this->where,
            groupBy: $this->groupBy,
            having: $this->having,
            windows: $this->windows,
            setOp: $this->setOp,
            setOpRhs: $this->setOpRhs,
            setOpLhs: $this->setOpLhs,
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: [...$this->locks, $lock],
        );
    }

    public function from(string|TableReference ...$tables): SelectJoinStep
    {
        $tables = array_map(static function (string|TableReference $t): TableReference {
            if ($t instanceof TableReference) {
                return $t;
            }
            $id = QualifiedIdentifier::parse($t);

            return new Table($id->name(), $id->schema());
        }, $tables);

        return new self(
            with: $this->with,
            selectList: $this->selectList,
            distinct: $this->distinct,
            distinctOn: $this->distinctOn,
            from: $tables,
            where: $this->where,
            groupBy: $this->groupBy,
            having: $this->having,
            windows: $this->windows,
            setOp: $this->setOp,
            setOpRhs: $this->setOpRhs,
            setOpLhs: $this->setOpLhs,
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function fullJoin(string|TableReference $table, Condition $on): SelectJoinStep
    {
        if (is_string($table)) {
            $id = QualifiedIdentifier::parse($table);
            $table = new Table($id->name(), $id->schema());
        }

        $join = new JoinedTable(
            left: $this->from[count($this->from) - 1],
            right: $table,
            joinType: JoinType::FULL,
            onCondition: $on,
        );

        return new self(
            with: $this->with,
            selectList: $this->selectList,
            distinct: $this->distinct,
            distinctOn: $this->distinctOn,
            from: $this->from,
            joins: [...$this->joins, $join],
            where: $this->where,
            groupBy: $this->groupBy,
            having: $this->having,
            windows: $this->windows,
            setOp: $this->setOp,
            setOpRhs: $this->setOpRhs,
            setOpLhs: $this->setOpLhs,
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function groupBy(string|Expression ...$expressions): SelectHavingStep
    {
        $expressions = array_map(static fn(string|Expression $e): Expression => $e instanceof Expression
            ? $e
            : Column::fromParts(QualifiedIdentifier::parse($e)->parts()), $expressions);

        return new self(
            with: $this->with,
            selectList: $this->selectList,
            distinct: $this->distinct,
            distinctOn: $this->distinctOn,
            from: $this->from,
            joins: $this->joins,
            where: $this->where,
            groupBy: $expressions,
            having: $this->having,
            windows: $this->windows,
            setOp: $this->setOp,
            setOpRhs: $this->setOpRhs,
            setOpLhs: $this->setOpLhs,
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function having(Condition $condition): SelectWindowStep
    {
        return new self(
            with: $this->with,
            selectList: $this->selectList,
            distinct: $this->distinct,
            distinctOn: $this->distinctOn,
            from: $this->from,
            joins: $this->joins,
            where: $this->where,
            groupBy: $this->groupBy,
            having: $condition,
            windows: $this->windows,
            setOp: $this->setOp,
            setOpRhs: $this->setOpRhs,
            setOpLhs: $this->setOpLhs,
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function intersect(SelectFinalStep $other): SelectSetOperationStep
    {
        return $this->setOperation(SetOperation::INTERSECT, $other);
    }

    public function intersectAll(SelectFinalStep $other): SelectSetOperationStep
    {
        return $this->setOperation(SetOperation::INTERSECT_ALL, $other);
    }

    public function join(string|TableReference $table, Condition $on): SelectJoinStep
    {
        if (is_string($table)) {
            $id = QualifiedIdentifier::parse($table);
            $table = new Table($id->name(), $id->schema());
        }

        $join = new JoinedTable(
            left: $this->from[count($this->from) - 1],
            right: $table,
            joinType: JoinType::INNER,
            onCondition: $on,
        );

        return new self(
            with: $this->with,
            selectList: $this->selectList,
            distinct: $this->distinct,
            distinctOn: $this->distinctOn,
            from: $this->from,
            joins: [...$this->joins, $join],
            where: $this->where,
            groupBy: $this->groupBy,
            having: $this->having,
            windows: $this->windows,
            setOp: $this->setOp,
            setOpRhs: $this->setOpRhs,
            setOpLhs: $this->setOpLhs,
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function leftJoin(string|TableReference $table, Condition $on): SelectJoinStep
    {
        if (is_string($table)) {
            $id = QualifiedIdentifier::parse($table);
            $table = new Table($id->name(), $id->schema());
        }

        $join = new JoinedTable(
            left: $this->from[count($this->from) - 1],
            right: $table,
            joinType: JoinType::LEFT,
            onCondition: $on,
        );

        return new self(
            with: $this->with,
            selectList: $this->selectList,
            distinct: $this->distinct,
            distinctOn: $this->distinctOn,
            from: $this->from,
            joins: [...$this->joins, $join],
            where: $this->where,
            groupBy: $this->groupBy,
            having: $this->having,
            windows: $this->windows,
            setOp: $this->setOp,
            setOpRhs: $this->setOpRhs,
            setOpLhs: $this->setOpLhs,
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function limit(int $limit): SelectOffsetStep
    {
        return new self(
            with: $this->with,
            selectList: $this->selectList,
            distinct: $this->distinct,
            distinctOn: $this->distinctOn,
            from: $this->from,
            joins: $this->joins,
            where: $this->where,
            groupBy: $this->groupBy,
            having: $this->having,
            windows: $this->windows,
            setOp: $this->setOp,
            setOpRhs: $this->setOpRhs,
            setOpLhs: $this->setOpLhs,
            orderBy: $this->orderBy,
            limit: $limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function offset(int $offset): SelectLockingStep
    {
        return new self(
            with: $this->with,
            selectList: $this->selectList,
            distinct: $this->distinct,
            distinctOn: $this->distinctOn,
            from: $this->from,
            joins: $this->joins,
            where: $this->where,
            groupBy: $this->groupBy,
            having: $this->having,
            windows: $this->windows,
            setOp: $this->setOp,
            setOpRhs: $this->setOpRhs,
            setOpLhs: $this->setOpLhs,
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $offset,
            locks: $this->locks,
        );
    }

    public function orderBy(OrderBy ...$items): SelectLimitStep
    {
        return new self(
            with: $this->with,
            selectList: $this->selectList,
            distinct: $this->distinct,
            distinctOn: $this->distinctOn,
            from: $this->from,
            joins: $this->joins,
            where: $this->where,
            groupBy: $this->groupBy,
            having: $this->having,
            windows: $this->windows,
            setOp: $this->setOp,
            setOpRhs: $this->setOpRhs,
            setOpLhs: $this->setOpLhs,
            orderBy: $items,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function rightJoin(string|TableReference $table, Condition $on): SelectJoinStep
    {
        if (is_string($table)) {
            $id = QualifiedIdentifier::parse($table);
            $table = new Table($id->name(), $id->schema());
        }

        $join = new JoinedTable(
            left: $this->from[count($this->from) - 1],
            right: $table,
            joinType: JoinType::RIGHT,
            onCondition: $on,
        );

        return new self(
            with: $this->with,
            selectList: $this->selectList,
            distinct: $this->distinct,
            distinctOn: $this->distinctOn,
            from: $this->from,
            joins: [...$this->joins, $join],
            where: $this->where,
            groupBy: $this->groupBy,
            having: $this->having,
            windows: $this->windows,
            setOp: $this->setOp,
            setOpRhs: $this->setOpRhs,
            setOpLhs: $this->setOpLhs,
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function select(string|Expression ...$expressions): self
    {
        $expressions = array_map(static fn(string|Expression $e): Expression => $e instanceof Expression
            ? $e
            : Column::fromParts(QualifiedIdentifier::parse($e)->parts()), $expressions);

        return new self(
            with: $this->with,
            selectList: $expressions,
            distinct: false,
            from: $this->from,
            joins: $this->joins,
            where: $this->where,
            groupBy: $this->groupBy,
            having: $this->having,
            windows: $this->windows,
            setOp: $this->setOp,
            setOpRhs: $this->setOpRhs,
            setOpLhs: $this->setOpLhs,
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function selectDistinct(string|Expression ...$expressions): SelectFromStep
    {
        $expressions = array_map(static fn(string|Expression $e): Expression => $e instanceof Expression
            ? $e
            : Column::fromParts(QualifiedIdentifier::parse($e)->parts()), $expressions);

        return new self(
            with: $this->with,
            selectList: $expressions,
            distinct: true,
            from: $this->from,
            joins: $this->joins,
            where: $this->where,
            groupBy: $this->groupBy,
            having: $this->having,
            windows: $this->windows,
            setOp: $this->setOp,
            setOpRhs: $this->setOpRhs,
            setOpLhs: $this->setOpLhs,
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function selectDistinctOn(
        array $distinctExpressions,
        string|Expression ...$selectExpressions,
    ): SelectFromStep {
        $coerce = static fn(string|Expression $e): Expression => $e instanceof Expression
            ? $e
            : Column::fromParts(QualifiedIdentifier::parse($e)->parts());

        return new self(
            with: $this->with,
            selectList: array_map($coerce, $selectExpressions),
            distinct: true,
            distinctOn: array_map($coerce, $distinctExpressions),
            from: $this->from,
            joins: $this->joins,
            where: $this->where,
            groupBy: $this->groupBy,
            having: $this->having,
            windows: $this->windows,
            setOp: $this->setOp,
            setOpRhs: $this->setOpRhs,
            setOpLhs: $this->setOpLhs,
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function toAst(): SelectStmt
    {
        if ($this->setOp !== null && $this->setOpRhs !== null) {
            return $this->buildSetOperationAst();
        }

        return $this->buildSimpleSelectAst();
    }

    public function union(SelectFinalStep $other): SelectSetOperationStep
    {
        return $this->setOperation(SetOperation::UNION, $other);
    }

    public function unionAll(SelectFinalStep $other): SelectSetOperationStep
    {
        return $this->setOperation(SetOperation::UNION_ALL, $other);
    }

    public function where(Condition|ConditionBuilder $condition): SelectGroupByStep
    {
        if ($condition instanceof ConditionBuilder) {
            $resolved = $condition->getCondition();

            if ($resolved === null) {
                throw InvalidBuilderStateException::emptyConditionBuilder();
            }

            $condition = $resolved;
        }

        return new self(
            with: $this->with,
            selectList: $this->selectList,
            distinct: $this->distinct,
            distinctOn: $this->distinctOn,
            from: $this->from,
            joins: $this->joins,
            where: $condition,
            groupBy: $this->groupBy,
            having: $this->having,
            windows: $this->windows,
            setOp: $this->setOp,
            setOpRhs: $this->setOpRhs,
            setOpLhs: $this->setOpLhs,
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function window(WindowDefinition ...$windows): SelectSetOperationStep
    {
        return new self(
            with: $this->with,
            selectList: $this->selectList,
            distinct: $this->distinct,
            distinctOn: $this->distinctOn,
            from: $this->from,
            joins: $this->joins,
            where: $this->where,
            groupBy: $this->groupBy,
            having: $this->having,
            windows: $windows,
            setOp: $this->setOp,
            setOpRhs: $this->setOpRhs,
            setOpLhs: $this->setOpLhs,
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    private function setOperation(SetOperation $op, SelectFinalStep $other): self
    {
        if ($this->setOp !== null) {
            // Already a set operation: nest the current query left-associatively as the left
            // operand (matching PostgreSQL's `a UNION b UNION c` => `(a UNION b) UNION c`) and
            // hoist the WITH clause to the new outer level.
            return new self(with: $this->with, setOp: $op, setOpRhs: $other, setOpLhs: $this->withoutWith());
        }

        return new self(
            with: $this->with,
            selectList: $this->selectList,
            distinct: $this->distinct,
            distinctOn: $this->distinctOn,
            from: $this->from,
            joins: $this->joins,
            where: $this->where,
            groupBy: $this->groupBy,
            having: $this->having,
            windows: $this->windows,
            setOp: $op,
            setOpRhs: $other,
        );
    }

    private function withoutWith(): self
    {
        return new self(
            with: null,
            selectList: $this->selectList,
            distinct: $this->distinct,
            distinctOn: $this->distinctOn,
            from: $this->from,
            joins: $this->joins,
            where: $this->where,
            groupBy: $this->groupBy,
            having: $this->having,
            windows: $this->windows,
            setOp: $this->setOp,
            setOpRhs: $this->setOpRhs,
            setOpLhs: $this->setOpLhs,
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    private function buildSetOperationAst(): SelectStmt
    {
        $selectStmt = new SelectStmt();

        if ($this->with !== null) {
            $withNode = $this->with->toAst();
            $withClause = $withNode->getWithClause();

            if ($withClause !== null) {
                $selectStmt->setWithClause($withClause);
            }
        }

        if ($this->setOp !== null) {
            $selectStmt->setOp($this->setOp->toProtobuf());
            $selectStmt->setAll($this->setOp->hasAll());
        }

        $leftStmt = $this->setOpLhs !== null ? $this->setOpLhs->toAst() : $this->buildSimpleSelectAst(false);
        $selectStmt->setLarg($leftStmt);

        if ($this->setOpRhs !== null) {
            $rightStmt = $this->setOpRhs->toAst();
            $selectStmt->setRarg($rightStmt);
        }

        if ($this->orderBy !== []) {
            $sortClause = [];

            foreach ($this->orderBy as $orderByItem) {
                $sortClause[] = $orderByItem->toNode();
            }

            $selectStmt->setSortClause($sortClause);
        }

        if ($this->limit !== null) {
            $selectStmt->setLimitCount(Literal::int($this->limit)->toAst());
            $selectStmt->setLimitOption(LimitOption::LIMIT_OPTION_COUNT);
        }

        if ($this->offset !== null) {
            $selectStmt->setLimitOffset(Literal::int($this->offset)->toAst());
        }

        if ($this->locks !== []) {
            $lockingClause = [];

            foreach ($this->locks as $lock) {
                $lockingClause[] = $lock->toAst();
            }

            $selectStmt->setLockingClause($lockingClause);
        }

        return $selectStmt;
    }

    private function buildSimpleSelectAst(bool $includeWith = true): SelectStmt
    {
        $selectStmt = new SelectStmt();

        if ($includeWith && $this->with !== null) {
            $withNode = $this->with->toAst();
            $withClause = $withNode->getWithClause();

            if ($withClause !== null) {
                $selectStmt->setWithClause($withClause);
            }
        }

        if ($this->distinct && $this->distinctOn !== []) {
            $distinctClause = [];

            foreach ($this->distinctOn as $expr) {
                $distinctClause[] = $expr->toAst();
            }

            $selectStmt->setDistinctClause($distinctClause);
        } elseif ($this->distinct) {
            $selectStmt->setDistinctClause([new Node()]);
        }

        $targetList = [];

        foreach ($this->selectList as $expr) {
            if ($expr instanceof AliasedExpression) {
                $targetList[] = $expr->toAst();
            } else {
                $resTarget = new ResTarget();
                $resTarget->setVal($expr->toAst());
                $node = new Node();
                $node->setResTarget($resTarget);
                $targetList[] = $node;
            }
        }

        $selectStmt->setTargetList($targetList);

        if ($this->from !== [] || $this->joins !== []) {
            $fromClause = [];

            if ($this->from !== []) {
                if ($this->joins !== []) {
                    $base = $this->from[0];
                    $current = $base;

                    foreach ($this->joins as $join) {
                        $joined = new JoinedTable(
                            left: $current,
                            right: $join->right,
                            joinType: $join->joinType,
                            onCondition: $join->onCondition,
                            usingColumns: $join->usingColumns,
                            natural: $join->natural,
                        );
                        $current = $joined;
                    }

                    $fromClause[] = $current->toAst();

                    for ($i = 1; $i < count($this->from); $i++) {
                        $fromClause[] = $this->from[$i]->toAst();
                    }
                } else {
                    foreach ($this->from as $table) {
                        $fromClause[] = $table->toAst();
                    }
                }
            }

            $selectStmt->setFromClause($fromClause);
        }

        if ($this->where !== null) {
            $selectStmt->setWhereClause($this->where->toAst());
        }

        if ($this->groupBy !== []) {
            $groupClause = [];

            foreach ($this->groupBy as $expr) {
                $groupClause[] = $expr->toAst();
            }

            $selectStmt->setGroupClause($groupClause);
        }

        if ($this->having !== null) {
            $selectStmt->setHavingClause($this->having->toAst());
        }

        if ($this->windows !== []) {
            $windowClause = [];

            foreach ($this->windows as $window) {
                $windowClause[] = $window->toAst();
            }

            $selectStmt->setWindowClause($windowClause);
        }

        if ($this->orderBy !== []) {
            $sortClause = [];

            foreach ($this->orderBy as $orderByItem) {
                $sortClause[] = $orderByItem->toNode();
            }

            $selectStmt->setSortClause($sortClause);
        }

        if ($this->limit !== null) {
            $selectStmt->setLimitCount(Literal::int($this->limit)->toAst());
            $selectStmt->setLimitOption(LimitOption::LIMIT_OPTION_COUNT);
        }

        if ($this->offset !== null) {
            $selectStmt->setLimitOffset(Literal::int($this->offset)->toAst());
        }

        if ($this->locks !== []) {
            $lockingClause = [];

            foreach ($this->locks as $lock) {
                $lockingClause[] = $lock->toAst();
            }

            $selectStmt->setLockingClause($lockingClause);
        }

        return $selectStmt;
    }

    /**
     * @return array{base: null|TableReference, joins: array<JoinedTable>}
     */
    private static function flattenJoins(Node $joinNode): array
    {
        $joined = JoinedTable::fromAst($joinNode);
        $joins = [];
        $base = null;

        $current = $joined;

        while ($current->left instanceof JoinedTable) {
            $joins[] = $current;
            $current = $current->left;
        }

        $base = $current->left;
        $joins[] = $current;

        $joins = array_reverse($joins);

        return [
            'base' => $base,
            'joins' => $joins,
        ];
    }

    private static function fromSetOperation(SelectStmt $selectStmt, ?WithClause $with): static
    {
        $op = $selectStmt->getOp();
        $all = $selectStmt->getAll();
        $setOp = SetOperation::fromProtobuf($op, $all);

        $larg = $selectStmt->getLarg();

        if ($larg === null) {
            throw InvalidAstException::missingRequiredField('larg', 'SelectStmt');
        }

        $left = self::fromAst($larg);

        $rarg = $selectStmt->getRarg();

        if ($rarg === null) {
            throw InvalidAstException::missingRequiredField('rarg', 'SelectStmt');
        }

        $right = self::fromAst($rarg);

        $orderBy = [];

        foreach ($selectStmt->getSortClause() as $sortNode) {
            $sortBy = $sortNode->getSortBy();

            if ($sortBy !== null) {
                $orderBy[] = OrderBy::fromAst($sortBy);
            }
        }

        $limit = null;
        $limitCount = $selectStmt->getLimitCount();

        if ($limitCount !== null) {
            $limitExpr = ExpressionFactory::fromAst($limitCount);

            if ($limitExpr instanceof Literal && $limitExpr->isInt()) {
                $limitValue = $limitExpr->value();
                $limit = is_int($limitValue) ? $limitValue : null;
            }
        }

        $offset = null;
        $limitOffset = $selectStmt->getLimitOffset();

        if ($limitOffset !== null) {
            $offsetExpr = ExpressionFactory::fromAst($limitOffset);

            if ($offsetExpr instanceof Literal && $offsetExpr->isInt()) {
                $offsetValue = $offsetExpr->value();
                $offset = is_int($offsetValue) ? $offsetValue : null;
            }
        }

        $locks = [];

        foreach ($selectStmt->getLockingClause() as $lockNode) {
            $locks[] = LockingClause::fromAst($lockNode);
        }

        return new self(
            with: $with,
            setOp: $setOp,
            setOpRhs: $right,
            setOpLhs: $left,
            orderBy: $orderBy,
            limit: $limit,
            offset: $offset,
            locks: $locks,
        );
    }

    private static function tableReferenceFromNode(Node $node): TableReference
    {
        if ($node->hasRangeVar()) {
            $rangeVar = $node->getRangeVar();

            if ($rangeVar === null) {
                throw InvalidAstException::missingRequiredField('range_var', 'Node');
            }

            return Table::fromAst($node);
        }

        if ($node->hasRangeSubselect()) {
            return DerivedTable::fromAst($node);
        }

        if ($node->hasRangeFunction()) {
            $rangeFunction = $node->getRangeFunction();
            $tableFunction = TableFunction::fromAst($node);

            if ($rangeFunction !== null) {
                $alias = $rangeFunction->getAlias();

                if ($alias !== null && $alias->getAliasname() !== '') {
                    $columnAliases = null;
                    $colnames = $alias->getColnames();

                    if (count($colnames) > 0) {
                        $columnAliases = [];

                        foreach ($colnames as $colNode) {
                            $colString = $colNode->getString();

                            if ($colString !== null) {
                                $columnAliases[] = $colString->getSval();
                            }
                        }
                    }

                    return new AliasedTable($tableFunction, $alias->getAliasname(), $columnAliases);
                }
            }

            return $tableFunction;
        }

        throw InvalidAstException::unexpectedNodeType('RangeVar, RangeSubselect or RangeFunction', 'unknown');
    }
}
