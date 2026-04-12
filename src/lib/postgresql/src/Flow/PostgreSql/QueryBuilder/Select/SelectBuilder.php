<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Select;

use Flow\PostgreSql\Protobuf\AST\{LimitOption, Node, ResTarget, SelectStmt as ProtobufSelectStmt};
use Flow\PostgreSql\QueryBuilder\{AstToSql, QualifiedIdentifier};
use Flow\PostgreSql\QueryBuilder\Clause\{LockingClause, OrderBy, WindowDefinition, WithClause};
use Flow\PostgreSql\QueryBuilder\Condition\{Condition, ConditionBuilder, ConditionFactory};
use Flow\PostgreSql\QueryBuilder\Exception\{InvalidAstException, InvalidBuilderStateException};
use Flow\PostgreSql\QueryBuilder\Expression\{AliasedExpression, Column, Expression, ExpressionFactory, Literal};
use Flow\PostgreSql\QueryBuilder\Table\{AliasedTable, DerivedTable, JoinType, JoinedTable, Table, TableFunction, TableReference};

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
        private array $orderBy = [],
        private ?int $limit = null,
        private ?int $offset = null,
        private array $locks = [],
    ) {
    }

    public static function create() : self
    {
        return new self();
    }

    public static function fromAst(ProtobufSelectStmt $selectStmt) : static
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
        $targetList = $selectStmt->getTargetList();

        if ($targetList !== null) {
            foreach ($targetList as $targetNode) {
                $resTarget = $targetNode->getResTarget();

                if ($resTarget !== null && $resTarget->getName() !== null && $resTarget->getName() !== '') {
                    $selectList[] = AliasedExpression::fromAst($targetNode);
                } else {
                    $valNode = $resTarget?->getVal();

                    if ($valNode !== null) {
                        $selectList[] = ExpressionFactory::fromAst($valNode);
                    }
                }
            }
        }

        $distinct = false;
        $distinctOn = [];
        $distinctClause = $selectStmt->getDistinctClause();

        if ($distinctClause !== null && \count($distinctClause) > 0) {
            $distinct = true;

            foreach ($distinctClause as $distinctNode) {
                if ($distinctNode->serializeToJsonString() !== '{}') {
                    $distinctOn[] = ExpressionFactory::fromAst($distinctNode);
                }
            }
        }

        $from = [];
        $joins = [];
        $fromClause = $selectStmt->getFromClause();

        if ($fromClause !== null) {
            foreach ($fromClause as $fromNode) {
                if ($fromNode->hasJoinExpr()) {
                    $flattenedJoins = self::flattenJoins($fromNode);
                    $base = $flattenedJoins['base'];

                    if ($base !== null) {
                        $from[] = $base;
                    }

                    $joins = \array_merge($joins, $flattenedJoins['joins']);
                } else {
                    $from[] = self::tableReferenceFromNode($fromNode);
                }
            }
        }

        $where = null;
        $whereClause = $selectStmt->getWhereClause();

        if ($whereClause !== null) {
            $where = ConditionFactory::fromAst($whereClause);
        }

        $groupBy = [];
        $groupClause = $selectStmt->getGroupClause();

        if ($groupClause !== null) {
            foreach ($groupClause as $groupNode) {
                $groupBy[] = ExpressionFactory::fromAst($groupNode);
            }
        }

        $having = null;
        $havingClause = $selectStmt->getHavingClause();

        if ($havingClause !== null) {
            $having = ConditionFactory::fromAst($havingClause);
        }

        $windows = [];
        $windowClause = $selectStmt->getWindowClause();

        if ($windowClause !== null) {
            foreach ($windowClause as $windowNode) {
                $windows[] = WindowDefinition::fromAst($windowNode);
            }
        }

        $orderBy = [];
        $sortClause = $selectStmt->getSortClause();

        if ($sortClause !== null) {
            foreach ($sortClause as $sortNode) {
                $sortBy = $sortNode->getSortBy();

                if ($sortBy !== null) {
                    $orderBy[] = OrderBy::fromAst($sortBy);
                }
            }
        }

        $limit = null;
        $limitCount = $selectStmt->getLimitCount();

        if ($limitCount !== null) {
            $limitExpr = ExpressionFactory::fromAst($limitCount);

            if ($limitExpr instanceof Literal && $limitExpr->isInt()) {
                $limitValue = $limitExpr->value();
                $limit = \is_int($limitValue) ? $limitValue : null;
            }
        }

        $offset = null;
        $limitOffset = $selectStmt->getLimitOffset();

        if ($limitOffset !== null) {
            $offsetExpr = ExpressionFactory::fromAst($limitOffset);

            if ($offsetExpr instanceof Literal && $offsetExpr->isInt()) {
                $offsetValue = $offsetExpr->value();
                $offset = \is_int($offsetValue) ? $offsetValue : null;
            }
        }

        $locks = [];
        $lockingClause = $selectStmt->getLockingClause();

        if ($lockingClause !== null) {
            foreach ($lockingClause as $lockNode) {
                $locks[] = LockingClause::fromAst($lockNode);
            }
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

    public static function with(WithClause $with) : SelectSelectStep
    {
        return new self(with: $with);
    }

    public function crossJoin(string|TableReference $table) : SelectJoinStep
    {
        if (\is_string($table)) {
            $id = QualifiedIdentifier::parse($table);
            $table = new Table($id->name(), $id->schema());
        }

        $join = new JoinedTable(
            left: $this->from[\count($this->from) - 1],
            right: $table,
            joinType: JoinType::CROSS,
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
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function except(SelectFinalStep $other) : SelectOrderByStep
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
            setOp: SetOperation::EXCEPT,
            setOpRhs: $other,
        );
    }

    public function exceptAll(SelectFinalStep $other) : SelectOrderByStep
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
            setOp: SetOperation::EXCEPT_ALL,
            setOpRhs: $other,
        );
    }

    public function forKeyShare(string ...$tables) : SelectFinalStep
    {
        $lock = LockingClause::forKeyShare(\array_values($tables));

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
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: [...$this->locks, $lock],
        );
    }

    public function forNoKeyUpdate(string ...$tables) : SelectFinalStep
    {
        $lock = LockingClause::forNoKeyUpdate(\array_values($tables));

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
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: [...$this->locks, $lock],
        );
    }

    public function forShare(string ...$tables) : SelectFinalStep
    {
        $lock = LockingClause::forShare(\array_values($tables));

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
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: [...$this->locks, $lock],
        );
    }

    public function forUpdate(string ...$tables) : SelectFinalStep
    {
        $lock = LockingClause::forUpdate(\array_values($tables));

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
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: [...$this->locks, $lock],
        );
    }

    public function forUpdateSkipLocked(string ...$tables) : SelectFinalStep
    {
        $lock = LockingClause::forUpdate(\array_values($tables))->skipLocked();

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
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: [...$this->locks, $lock],
        );
    }

    public function from(string|TableReference ...$tables) : SelectJoinStep
    {
        $tables = \array_map(
            static function (string|TableReference $t) : TableReference {
                if ($t instanceof TableReference) {
                    return $t;
                }
                $id = QualifiedIdentifier::parse($t);

                return new Table($id->name(), $id->schema());
            },
            $tables,
        );

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
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function fullJoin(string|TableReference $table, Condition $on) : SelectJoinStep
    {
        if (\is_string($table)) {
            $id = QualifiedIdentifier::parse($table);
            $table = new Table($id->name(), $id->schema());
        }

        $join = new JoinedTable(
            left: $this->from[\count($this->from) - 1],
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
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function groupBy(string|Expression ...$expressions) : SelectHavingStep
    {
        $expressions = \array_map(
            static fn (string|Expression $e) : Expression => $e instanceof Expression ? $e : Column::fromParts(QualifiedIdentifier::parse($e)->parts()),
            $expressions,
        );

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
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function having(Condition $condition) : SelectWindowStep
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
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function intersect(SelectFinalStep $other) : SelectOrderByStep
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
            setOp: SetOperation::INTERSECT,
            setOpRhs: $other,
        );
    }

    public function intersectAll(SelectFinalStep $other) : SelectOrderByStep
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
            setOp: SetOperation::INTERSECT_ALL,
            setOpRhs: $other,
        );
    }

    public function join(string|TableReference $table, Condition $on) : SelectJoinStep
    {
        if (\is_string($table)) {
            $id = QualifiedIdentifier::parse($table);
            $table = new Table($id->name(), $id->schema());
        }

        $join = new JoinedTable(
            left: $this->from[\count($this->from) - 1],
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
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function leftJoin(string|TableReference $table, Condition $on) : SelectJoinStep
    {
        if (\is_string($table)) {
            $id = QualifiedIdentifier::parse($table);
            $table = new Table($id->name(), $id->schema());
        }

        $join = new JoinedTable(
            left: $this->from[\count($this->from) - 1],
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
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function limit(int $limit) : SelectOffsetStep
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
            orderBy: $this->orderBy,
            limit: $limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function offset(int $offset) : SelectLockingStep
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
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $offset,
            locks: $this->locks,
        );
    }

    public function orderBy(OrderBy ...$items) : SelectLimitStep
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
            orderBy: $items,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function rightJoin(string|TableReference $table, Condition $on) : SelectJoinStep
    {
        if (\is_string($table)) {
            $id = QualifiedIdentifier::parse($table);
            $table = new Table($id->name(), $id->schema());
        }

        $join = new JoinedTable(
            left: $this->from[\count($this->from) - 1],
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
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function select(string|Expression ...$expressions) : self
    {
        $expressions = \array_map(
            static fn (string|Expression $e) : Expression => $e instanceof Expression ? $e : Column::fromParts(QualifiedIdentifier::parse($e)->parts()),
            $expressions,
        );

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
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function selectDistinct(string|Expression ...$expressions) : SelectFromStep
    {
        $expressions = \array_map(
            static fn (string|Expression $e) : Expression => $e instanceof Expression ? $e : Column::fromParts(QualifiedIdentifier::parse($e)->parts()),
            $expressions,
        );

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
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function selectDistinctOn(array $distinctExpressions, string|Expression ...$selectExpressions) : SelectFromStep
    {
        $coerce = static fn (string|Expression $e) : Expression => $e instanceof Expression ? $e : Column::fromParts(QualifiedIdentifier::parse($e)->parts());

        return new self(
            with: $this->with,
            selectList: \array_map($coerce, $selectExpressions),
            distinct: true,
            distinctOn: \array_map($coerce, $distinctExpressions),
            from: $this->from,
            joins: $this->joins,
            where: $this->where,
            groupBy: $this->groupBy,
            having: $this->having,
            windows: $this->windows,
            setOp: $this->setOp,
            setOpRhs: $this->setOpRhs,
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function toAst() : ProtobufSelectStmt
    {
        if ($this->setOp !== null && $this->setOpRhs !== null) {
            return $this->buildSetOperationAst();
        }

        return $this->buildSimpleSelectAst();
    }

    public function union(SelectFinalStep $other) : SelectOrderByStep
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
            setOp: SetOperation::UNION,
            setOpRhs: $other,
        );
    }

    public function unionAll(SelectFinalStep $other) : SelectOrderByStep
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
            setOp: SetOperation::UNION_ALL,
            setOpRhs: $other,
        );
    }

    public function where(Condition|ConditionBuilder $condition) : SelectGroupByStep
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
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    public function window(WindowDefinition ...$windows) : SelectSetOperationStep
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
            orderBy: $this->orderBy,
            limit: $this->limit,
            offset: $this->offset,
            locks: $this->locks,
        );
    }

    private function buildSetOperationAst() : ProtobufSelectStmt
    {
        $selectStmt = new ProtobufSelectStmt();

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

        $leftStmt = $this->buildSimpleSelectAst();
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

    private function buildSimpleSelectAst() : ProtobufSelectStmt
    {
        $selectStmt = new ProtobufSelectStmt();

        if ($this->with !== null) {
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

                    for ($i = 1; $i < \count($this->from); $i++) {
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
    private static function flattenJoins(Node $joinNode) : array
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

        $joins = \array_reverse($joins);

        return [
            'base' => $base,
            'joins' => $joins,
        ];
    }

    private static function fromSetOperation(ProtobufSelectStmt $selectStmt, ?WithClause $with) : static
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
        $sortClause = $selectStmt->getSortClause();

        if ($sortClause !== null) {
            foreach ($sortClause as $sortNode) {
                $sortBy = $sortNode->getSortBy();

                if ($sortBy !== null) {
                    $orderBy[] = OrderBy::fromAst($sortBy);
                }
            }
        }

        $limit = null;
        $limitCount = $selectStmt->getLimitCount();

        if ($limitCount !== null) {
            $limitExpr = ExpressionFactory::fromAst($limitCount);

            if ($limitExpr instanceof Literal && $limitExpr->isInt()) {
                $limitValue = $limitExpr->value();
                $limit = \is_int($limitValue) ? $limitValue : null;
            }
        }

        $offset = null;
        $limitOffset = $selectStmt->getLimitOffset();

        if ($limitOffset !== null) {
            $offsetExpr = ExpressionFactory::fromAst($limitOffset);

            if ($offsetExpr instanceof Literal && $offsetExpr->isInt()) {
                $offsetValue = $offsetExpr->value();
                $offset = \is_int($offsetValue) ? $offsetValue : null;
            }
        }

        $locks = [];
        $lockingClause = $selectStmt->getLockingClause();

        if ($lockingClause !== null) {
            foreach ($lockingClause as $lockNode) {
                $locks[] = LockingClause::fromAst($lockNode);
            }
        }

        return new self(
            with: $with,
            selectList: $left->selectList,
            distinct: $left->distinct,
            distinctOn: $left->distinctOn,
            from: $left->from,
            joins: $left->joins,
            where: $left->where,
            groupBy: $left->groupBy,
            having: $left->having,
            windows: $left->windows,
            setOp: $setOp,
            setOpRhs: $right,
            orderBy: $orderBy,
            limit: $limit,
            offset: $offset,
            locks: $locks,
        );
    }

    private static function tableReferenceFromNode(Node $node) : TableReference
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

                    if ($colnames !== null && \count($colnames) > 0) {
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
