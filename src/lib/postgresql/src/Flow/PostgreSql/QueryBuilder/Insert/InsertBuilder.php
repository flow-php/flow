<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Insert;

use Flow\PostgreSql\Protobuf\AST\Alias;
use Flow\PostgreSql\Protobuf\AST\InsertStmt;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBList;
use Flow\PostgreSql\Protobuf\AST\RangeVar;
use Flow\PostgreSql\Protobuf\AST\ResTarget;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use Flow\PostgreSql\QueryBuilder\AstToSql;
use Flow\PostgreSql\QueryBuilder\Clause\ConflictTarget;
use Flow\PostgreSql\QueryBuilder\Clause\OnConflictClause;
use Flow\PostgreSql\QueryBuilder\Clause\WithClause;
use Flow\PostgreSql\QueryBuilder\Condition\Condition;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;
use Flow\PostgreSql\QueryBuilder\Expression\ExpressionFactory;
use Flow\PostgreSql\QueryBuilder\Expression\Star;
use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;
use Flow\PostgreSql\QueryBuilder\Select\SelectFinalStep;
use Flow\PostgreSql\QueryBuilder\Table\Table;

final readonly class InsertBuilder implements InsertColumnsStep, InsertDoUpdateStep, InsertIntoStep
{
    use AstToSql;

    /**
     * @param list<string> $columns
     * @param list<list<Expression>> $valuesList
     * @param list<Expression> $returning
     */
    private function __construct(
        private ?WithClause $with = null,
        private ?string $table = null,
        private ?string $schema = null,
        private ?string $alias = null,
        private array $columns = [],
        private array $valuesList = [],
        private ?SelectFinalStep $selectQuery = null,
        private bool $defaultValues = false,
        private ?OnConflictClause $onConflict = null,
        private array $returning = [],
        private bool $returningAll = false,
    ) {}

    public static function create(): InsertIntoStep
    {
        return new self();
    }

    public static function fromAst(InsertStmt $insertStmt): static
    {
        $relation = $insertStmt->getRelation();

        if ($relation === null) {
            throw InvalidAstException::missingRequiredField('relation', 'InsertStmt');
        }

        $table = $relation->getRelname();

        if ($table === '') {
            throw InvalidAstException::missingRequiredField('relname', 'RangeVar');
        }

        $schema = $relation->getSchemaname();

        if ($schema === '') {
            $schema = null;
        }

        $alias = $relation->getAlias();

        if ($alias !== null) {
            $aliasName = $alias->getAliasname();

            if ($aliasName === '') {
                $aliasName = null;
            }
        } else {
            $aliasName = null;
        }

        $columns = [];
        $colsNodes = $insertStmt->getCols();

        foreach ($colsNodes as $colNode) {
            $resTarget = $colNode->getResTarget();

            if ($resTarget !== null) {
                $name = $resTarget->getName();

                if ($name !== '') {
                    $columns[] = $name;
                }
            }
        }

        $valuesList = [];
        $selectQuery = null;
        $defaultValues = false;
        $selectStmtNode = $insertStmt->getSelectStmt();

        if ($selectStmtNode !== null) {
            $selectStmt = $selectStmtNode->getSelectStmt();

            if ($selectStmt !== null) {
                $valuesListsNodes = $selectStmt->getValuesLists();

                if (\count($valuesListsNodes) > 0) {
                    foreach ($valuesListsNodes as $listNode) {
                        $list = $listNode->getList();

                        if ($list !== null) {
                            $rowValues = [];
                            $elements = $list->getItems();

                            foreach ($elements as $element) {
                                $rowValues[] = ExpressionFactory::fromAst($element);
                            }

                            $valuesList[] = $rowValues;
                        }
                    }
                } else {
                    $targetList = $selectStmt->getTargetList();

                    if (\count($targetList) === 0) {
                        $defaultValues = true;
                    } else {
                        $selectQuery = new readonly class($selectStmt) implements SelectFinalStep {
                            use AstToSql;

                            public function __construct(
                                private SelectStmt $stmt,
                            ) {}

                            public static function fromAst(Node $node): static
                            {
                                $selectStmt = $node->getSelectStmt();

                                if ($selectStmt === null) {
                                    throw InvalidAstException::unexpectedNodeType('SelectStmt', 'unknown');
                                }

                                return new self($selectStmt);
                            }

                            public function toAst(): SelectStmt
                            {
                                return $this->stmt;
                            }
                        };
                    }
                }
            }
        }

        $onConflict = null;
        $onConflictNode = $insertStmt->getOnConflictClause();

        if ($onConflictNode !== null) {
            $conflictNode = new Node();
            $conflictNode->setOnConflictClause($onConflictNode);
            $onConflict = OnConflictClause::fromAst($conflictNode);
        }

        $returning = [];
        $returningAll = false;
        $returningListNodes = $insertStmt->getReturningList();

        foreach ($returningListNodes as $retNode) {
            $resTarget = $retNode->getResTarget();

            if ($resTarget !== null) {
                $valNode = $resTarget->getVal();

                if ($valNode !== null) {
                    $expr = ExpressionFactory::fromAst($valNode);

                    if ($expr instanceof Star && !$expr->isQualified()) {
                        $returningAll = true;
                        $returning = [];

                        break;
                    }

                    $returning[] = $expr;
                }
            }
        }

        $with = null;
        $withClauseNode = $insertStmt->getWithClause();

        if ($withClauseNode !== null) {
            $with = WithClause::fromAst($withClauseNode);
        }

        return new self(
            $with,
            $table,
            $schema,
            $aliasName,
            $columns,
            $valuesList,
            $selectQuery,
            $defaultValues,
            $onConflict,
            $returning,
            $returningAll,
        );
    }

    public static function with(WithClause $with): InsertIntoStep
    {
        return new self(with: $with);
    }

    public function columns(string ...$columns): InsertValuesStep
    {
        return new self(
            $this->with,
            $this->table,
            $this->schema,
            $this->alias,
            \array_values([...$columns]),
            $this->valuesList,
            $this->selectQuery,
            $this->defaultValues,
            $this->onConflict,
            $this->returning,
            $this->returningAll,
        );
    }

    public function defaultValues(): InsertOnConflictStep
    {
        return new self(
            $this->with,
            $this->table,
            $this->schema,
            $this->alias,
            $this->columns,
            [],
            null,
            true,
            $this->onConflict,
            $this->returning,
            $this->returningAll,
        );
    }

    public function into(string|Table $table, ?string $alias = null): InsertColumnsStep
    {
        if ($table instanceof Table) {
            $name = $table->name;
            $schema = $table->schema;
        } else {
            $identifier = QualifiedIdentifier::parse($table);
            $name = $identifier->name();
            $schema = $identifier->schema();
        }

        return new self(
            $this->with,
            $name,
            $schema,
            $alias,
            $this->columns,
            $this->valuesList,
            $this->selectQuery,
            $this->defaultValues,
            $this->onConflict,
            $this->returning,
            $this->returningAll,
        );
    }

    public function onConflict(OnConflictClause $clause): InsertReturningStep
    {
        return new self(
            $this->with,
            $this->table,
            $this->schema,
            $this->alias,
            $this->columns,
            $this->valuesList,
            $this->selectQuery,
            $this->defaultValues,
            $clause,
            $this->returning,
            $this->returningAll,
        );
    }

    public function onConflictDoNothing(?ConflictTarget $target = null): InsertReturningStep
    {
        return $this->onConflict(OnConflictClause::doNothing($target));
    }

    public function onConflictDoUpdate(ConflictTarget $target, array $updates): InsertDoUpdateStep
    {
        return new self(
            $this->with,
            $this->table,
            $this->schema,
            $this->alias,
            $this->columns,
            $this->valuesList,
            $this->selectQuery,
            $this->defaultValues,
            OnConflictClause::doUpdate($target, $updates),
            $this->returning,
            $this->returningAll,
        );
    }

    public function returning(Expression ...$expressions): InsertFinalStep
    {
        return new self(
            $this->with,
            $this->table,
            $this->schema,
            $this->alias,
            $this->columns,
            $this->valuesList,
            $this->selectQuery,
            $this->defaultValues,
            $this->onConflict,
            \array_values([...$expressions]),
            false,
        );
    }

    public function returningAll(): InsertFinalStep
    {
        return new self(
            $this->with,
            $this->table,
            $this->schema,
            $this->alias,
            $this->columns,
            $this->valuesList,
            $this->selectQuery,
            $this->defaultValues,
            $this->onConflict,
            [],
            true,
        );
    }

    public function select(SelectFinalStep $select): InsertOnConflictStep
    {
        return new self(
            $this->with,
            $this->table,
            $this->schema,
            $this->alias,
            $this->columns,
            [],
            $select,
            false,
            $this->onConflict,
            $this->returning,
            $this->returningAll,
        );
    }

    public function toAst(): InsertStmt
    {
        $insertStmt = new InsertStmt();

        $rangeVar = new RangeVar([
            'relname' => $this->table ?? '',
            'inh' => true,
        ]);

        if ($this->schema !== null) {
            $rangeVar->setSchemaname($this->schema);
        }

        if ($this->alias !== null) {
            $alias = new Alias();
            $alias->setAliasname($this->alias);
            $rangeVar->setAlias($alias);
        }

        $insertStmt->setRelation($rangeVar);

        if ($this->columns !== []) {
            $colsNodes = [];

            foreach ($this->columns as $column) {
                $resTarget = new ResTarget();
                $resTarget->setName($column);

                $colNode = new Node();
                $colNode->setResTarget($resTarget);

                $colsNodes[] = $colNode;
            }

            $insertStmt->setCols($colsNodes);
        }

        if ($this->selectQuery !== null) {
            $selectNode = new Node();
            $selectNode->setSelectStmt($this->selectQuery->toAst());
            $insertStmt->setSelectStmt($selectNode);
        } elseif ($this->defaultValues) {
            // DEFAULT VALUES: do not set selectStmt at all
        } elseif ($this->valuesList !== []) {
            $selectStmt = new SelectStmt();
            $valuesListsNodes = [];

            foreach ($this->valuesList as $values) {
                $items = [];

                foreach ($values as $expr) {
                    $items[] = $expr->toAst();
                }

                $list = new PBList();
                $list->setItems($items);

                $listNode = new Node();
                $listNode->setList($list);

                $valuesListsNodes[] = $listNode;
            }

            $selectStmt->setValuesLists($valuesListsNodes);

            $selectNode = new Node();
            $selectNode->setSelectStmt($selectStmt);
            $insertStmt->setSelectStmt($selectNode);
        }

        if ($this->onConflict !== null) {
            $onConflictNode = $this->onConflict->toAst();
            $onConflictClause = $onConflictNode->getOnConflictClause();

            if ($onConflictClause !== null) {
                $insertStmt->setOnConflictClause($onConflictClause);
            }
        }

        if ($this->returningAll) {
            $resTarget = new ResTarget();
            $resTarget->setVal(Star::all()->toAst());

            $retNode = new Node();
            $retNode->setResTarget($resTarget);

            $insertStmt->setReturningList([$retNode]);
        } elseif ($this->returning !== []) {
            $returningNodes = [];

            foreach ($this->returning as $expr) {
                $resTarget = new ResTarget();
                $resTarget->setVal($expr->toAst());

                $retNode = new Node();
                $retNode->setResTarget($resTarget);

                $returningNodes[] = $retNode;
            }

            $insertStmt->setReturningList($returningNodes);
        }

        if ($this->with !== null) {
            $withNode = $this->with->toAst();
            $withClause = $withNode->getWithClause();

            if ($withClause !== null) {
                $insertStmt->setWithClause($withClause);
            }
        }

        return $insertStmt;
    }

    public function values(Expression ...$values): InsertValuesStep
    {
        return new self(
            $this->with,
            $this->table,
            $this->schema,
            $this->alias,
            $this->columns,
            [...$this->valuesList, \array_values([...$values])],
            $this->selectQuery,
            $this->defaultValues,
            $this->onConflict,
            $this->returning,
            $this->returningAll,
        );
    }

    public function where(Condition $condition): InsertReturningStep
    {
        if ($this->onConflict === null) {
            return $this;
        }

        return new self(
            $this->with,
            $this->table,
            $this->schema,
            $this->alias,
            $this->columns,
            $this->valuesList,
            $this->selectQuery,
            $this->defaultValues,
            $this->onConflict->where($condition),
            $this->returning,
            $this->returningAll,
        );
    }
}
