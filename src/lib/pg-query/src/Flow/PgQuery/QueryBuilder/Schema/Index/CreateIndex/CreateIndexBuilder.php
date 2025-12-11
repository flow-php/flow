<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Index\CreateIndex;

use Flow\PgQuery\Protobuf\AST\{IndexElem, IndexStmt, Node, RangeVar};
use Flow\PgQuery\QueryBuilder\AstToSql;
use Flow\PgQuery\QueryBuilder\Condition\Condition;
use Flow\PgQuery\QueryBuilder\Schema\Index\{IndexColumn, IndexMethod};

final readonly class CreateIndexBuilder implements CreateIndexColumnsStep, CreateIndexFinalStep, CreateIndexOnStep
{
    use AstToSql;

    /**
     * @param list<IndexColumn> $columns
     * @param list<string> $includeColumns
     */
    private function __construct(
        private string $name,
        private ?string $table = null,
        private ?string $schema = null,
        private array $columns = [],
        private bool $unique = false,
        private bool $concurrent = false,
        private bool $ifNotExists = false,
        private bool $onlyTable = false,
        private ?IndexMethod $method = null,
        private array $includeColumns = [],
        private ?Condition $whereCondition = null,
        private ?string $tablespace = null,
        private bool $nullsNotDistinct = false,
    ) {
    }

    public static function create(string $name) : CreateIndexOnStep
    {
        return new self($name);
    }

    public function columns(IndexColumn|string ...$columns) : CreateIndexFinalStep
    {
        $indexColumns = [];

        foreach ($columns as $column) {
            if (\is_string($column)) {
                $indexColumns[] = IndexColumn::column($column);
            } else {
                $indexColumns[] = $column;
            }
        }

        return new self(
            $this->name,
            $this->table,
            $this->schema,
            $indexColumns,
            $this->unique,
            $this->concurrent,
            $this->ifNotExists,
            $this->onlyTable,
            $this->method,
            $this->includeColumns,
            $this->whereCondition,
            $this->tablespace,
            $this->nullsNotDistinct,
        );
    }

    public function concurrently() : CreateIndexOnStep
    {
        return new self(
            $this->name,
            $this->table,
            $this->schema,
            $this->columns,
            $this->unique,
            true,
            $this->ifNotExists,
            $this->onlyTable,
            $this->method,
            $this->includeColumns,
            $this->whereCondition,
            $this->tablespace,
            $this->nullsNotDistinct,
        );
    }

    public function ifNotExists() : CreateIndexOnStep
    {
        return new self(
            $this->name,
            $this->table,
            $this->schema,
            $this->columns,
            $this->unique,
            $this->concurrent,
            true,
            $this->onlyTable,
            $this->method,
            $this->includeColumns,
            $this->whereCondition,
            $this->tablespace,
            $this->nullsNotDistinct,
        );
    }

    public function include(string ...$columns) : CreateIndexFinalStep
    {
        return new self(
            $this->name,
            $this->table,
            $this->schema,
            $this->columns,
            $this->unique,
            $this->concurrent,
            $this->ifNotExists,
            $this->onlyTable,
            $this->method,
            \array_values([...$this->includeColumns, ...$columns]),
            $this->whereCondition,
            $this->tablespace,
            $this->nullsNotDistinct,
        );
    }

    public function nullsDistinct() : CreateIndexFinalStep
    {
        return new self(
            $this->name,
            $this->table,
            $this->schema,
            $this->columns,
            $this->unique,
            $this->concurrent,
            $this->ifNotExists,
            $this->onlyTable,
            $this->method,
            $this->includeColumns,
            $this->whereCondition,
            $this->tablespace,
            false,
        );
    }

    public function nullsNotDistinct() : CreateIndexFinalStep
    {
        return new self(
            $this->name,
            $this->table,
            $this->schema,
            $this->columns,
            $this->unique,
            $this->concurrent,
            $this->ifNotExists,
            $this->onlyTable,
            $this->method,
            $this->includeColumns,
            $this->whereCondition,
            $this->tablespace,
            true,
        );
    }

    public function on(string $table, ?string $schema = null) : CreateIndexColumnsStep
    {
        return new self(
            $this->name,
            $table,
            $schema,
            $this->columns,
            $this->unique,
            $this->concurrent,
            $this->ifNotExists,
            false,
            $this->method,
            $this->includeColumns,
            $this->whereCondition,
            $this->tablespace,
            $this->nullsNotDistinct,
        );
    }

    public function onOnly(string $table, ?string $schema = null) : CreateIndexColumnsStep
    {
        return new self(
            $this->name,
            $table,
            $schema,
            $this->columns,
            $this->unique,
            $this->concurrent,
            $this->ifNotExists,
            true,
            $this->method,
            $this->includeColumns,
            $this->whereCondition,
            $this->tablespace,
            $this->nullsNotDistinct,
        );
    }

    public function tablespace(string $tablespace) : CreateIndexFinalStep
    {
        return new self(
            $this->name,
            $this->table,
            $this->schema,
            $this->columns,
            $this->unique,
            $this->concurrent,
            $this->ifNotExists,
            $this->onlyTable,
            $this->method,
            $this->includeColumns,
            $this->whereCondition,
            $tablespace,
            $this->nullsNotDistinct,
        );
    }

    public function toAst() : IndexStmt
    {
        $stmt = new IndexStmt();

        $stmt->setIdxname($this->name);

        if ($this->table !== null) {
            $rangeVar = new RangeVar();
            $rangeVar->setRelname($this->table);
            $rangeVar->setInh(!$this->onlyTable);
            $rangeVar->setRelpersistence('p');

            if ($this->schema !== null) {
                $rangeVar->setSchemaname($this->schema);
            }

            $stmt->setRelation($rangeVar);
        }

        if ($this->unique) {
            $stmt->setUnique(true);
        }

        if ($this->concurrent) {
            $stmt->setConcurrent(true);
        }

        if ($this->ifNotExists) {
            $stmt->setIfNotExists(true);
        }

        if ($this->method !== null) {
            $stmt->setAccessMethod($this->method->value);
        }

        if ($this->columns !== []) {
            $indexParams = [];

            foreach ($this->columns as $column) {
                $node = new Node();
                $node->setIndexElem($column->toAst());
                $indexParams[] = $node;
            }

            $stmt->setIndexParams($indexParams);
        }

        if ($this->includeColumns !== []) {
            $includeParams = [];

            foreach ($this->includeColumns as $columnName) {
                $indexElem = new IndexElem();
                $indexElem->setName($columnName);

                $node = new Node();
                $node->setIndexElem($indexElem);
                $includeParams[] = $node;
            }

            $stmt->setIndexIncludingParams($includeParams);
        }

        if ($this->whereCondition !== null) {
            $stmt->setWhereClause($this->whereCondition->toAst());
        }

        if ($this->tablespace !== null) {
            $stmt->setTableSpace($this->tablespace);
        }

        if ($this->nullsNotDistinct) {
            $stmt->setNullsNotDistinct(true);
        }

        return $stmt;
    }

    public function unique() : CreateIndexOnStep
    {
        return new self(
            $this->name,
            $this->table,
            $this->schema,
            $this->columns,
            true,
            $this->concurrent,
            $this->ifNotExists,
            $this->onlyTable,
            $this->method,
            $this->includeColumns,
            $this->whereCondition,
            $this->tablespace,
            $this->nullsNotDistinct,
        );
    }

    public function using(IndexMethod $method) : CreateIndexColumnsStep
    {
        return new self(
            $this->name,
            $this->table,
            $this->schema,
            $this->columns,
            $this->unique,
            $this->concurrent,
            $this->ifNotExists,
            $this->onlyTable,
            $method,
            $this->includeColumns,
            $this->whereCondition,
            $this->tablespace,
            $this->nullsNotDistinct,
        );
    }

    public function where(Condition $predicate) : CreateIndexFinalStep
    {
        return new self(
            $this->name,
            $this->table,
            $this->schema,
            $this->columns,
            $this->unique,
            $this->concurrent,
            $this->ifNotExists,
            $this->onlyTable,
            $this->method,
            $this->includeColumns,
            $predicate,
            $this->tablespace,
            $this->nullsNotDistinct,
        );
    }
}
