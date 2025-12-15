<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\CreateTable;

use Flow\PostgreSql\Protobuf\AST\{CreateStmt, Node, OnCommitAction, PartitionElem, PartitionSpec, PartitionStrategy, RangeVar};
use Flow\PostgreSql\QueryBuilder\AstToSql;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnDefinition;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\TableConstraint;

final readonly class CreateTableBuilder implements CreateTableColumnsStep, CreateTableTemporaryStep, CreateTemporaryTableColumnsStep
{
    use AstToSql;

    /**
     * @param list<ColumnDefinition> $columns
     * @param list<TableConstraint> $constraints
     * @param list<string> $inherits
     * @param list<string> $partitionColumns
     */
    private function __construct(
        private string $table,
        private ?string $schema = null,
        private array $columns = [],
        private array $constraints = [],
        private bool $ifNotExists = false,
        private bool $temporary = false,
        private bool $unlogged = false,
        private array $inherits = [],
        private ?int $partitionStrategy = null,
        private array $partitionColumns = [],
        private ?string $tablespace = null,
        private ?int $onCommitAction = null,
    ) {
    }

    public static function create(string $table, ?string $schema = null) : CreateTableColumnsStep
    {
        return new self($table, $schema);
    }

    public static function createTemporary(string $table, ?string $schema = null) : CreateTemporaryTableColumnsStep
    {
        return new self($table, $schema, temporary: true);
    }

    public function column(ColumnDefinition $column) : self
    {
        return new self(
            $this->table,
            $this->schema,
            [...$this->columns, $column],
            $this->constraints,
            $this->ifNotExists,
            $this->temporary,
            $this->unlogged,
            $this->inherits,
            $this->partitionStrategy,
            $this->partitionColumns,
            $this->tablespace,
            $this->onCommitAction,
        );
    }

    public function constraint(TableConstraint $constraint) : CreateTableFinalStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            [...$this->constraints, $constraint],
            $this->ifNotExists,
            $this->temporary,
            $this->unlogged,
            $this->inherits,
            $this->partitionStrategy,
            $this->partitionColumns,
            $this->tablespace,
            $this->onCommitAction,
        );
    }

    public function ifNotExists() : CreateTableFinalStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->constraints,
            true,
            $this->temporary,
            $this->unlogged,
            $this->inherits,
            $this->partitionStrategy,
            $this->partitionColumns,
            $this->tablespace,
            $this->onCommitAction,
        );
    }

    public function inherits(string ...$tables) : CreateTableFinalStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->constraints,
            $this->ifNotExists,
            $this->temporary,
            $this->unlogged,
            \array_values([...$this->inherits, ...$tables]),
            $this->partitionStrategy,
            $this->partitionColumns,
            $this->tablespace,
            $this->onCommitAction,
        );
    }

    public function onCommitDeleteRows() : CreateTableFinalStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->constraints,
            $this->ifNotExists,
            $this->temporary,
            $this->unlogged,
            $this->inherits,
            $this->partitionStrategy,
            $this->partitionColumns,
            $this->tablespace,
            OnCommitAction::ONCOMMIT_DELETE_ROWS,
        );
    }

    public function onCommitDrop() : CreateTableFinalStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->constraints,
            $this->ifNotExists,
            $this->temporary,
            $this->unlogged,
            $this->inherits,
            $this->partitionStrategy,
            $this->partitionColumns,
            $this->tablespace,
            OnCommitAction::ONCOMMIT_DROP,
        );
    }

    public function onCommitPreserveRows() : CreateTableFinalStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->constraints,
            $this->ifNotExists,
            $this->temporary,
            $this->unlogged,
            $this->inherits,
            $this->partitionStrategy,
            $this->partitionColumns,
            $this->tablespace,
            OnCommitAction::ONCOMMIT_PRESERVE_ROWS,
        );
    }

    public function partitionByHash(string ...$columns) : CreateTableFinalStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->constraints,
            $this->ifNotExists,
            $this->temporary,
            $this->unlogged,
            $this->inherits,
            PartitionStrategy::PARTITION_STRATEGY_HASH,
            \array_values($columns),
            $this->tablespace,
            $this->onCommitAction,
        );
    }

    public function partitionByList(string ...$columns) : CreateTableFinalStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->constraints,
            $this->ifNotExists,
            $this->temporary,
            $this->unlogged,
            $this->inherits,
            PartitionStrategy::PARTITION_STRATEGY_LIST,
            \array_values($columns),
            $this->tablespace,
            $this->onCommitAction,
        );
    }

    public function partitionByRange(string ...$columns) : CreateTableFinalStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->constraints,
            $this->ifNotExists,
            $this->temporary,
            $this->unlogged,
            $this->inherits,
            PartitionStrategy::PARTITION_STRATEGY_RANGE,
            \array_values($columns),
            $this->tablespace,
            $this->onCommitAction,
        );
    }

    public function tablespace(string $tablespaceName) : CreateTableFinalStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->constraints,
            $this->ifNotExists,
            $this->temporary,
            $this->unlogged,
            $this->inherits,
            $this->partitionStrategy,
            $this->partitionColumns,
            $tablespaceName,
            $this->onCommitAction,
        );
    }

    public function temporary() : CreateTableTemporaryStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->constraints,
            $this->ifNotExists,
            true,
            false,
            $this->inherits,
            $this->partitionStrategy,
            $this->partitionColumns,
            $this->tablespace,
            null,
        );
    }

    public function toAst() : CreateStmt
    {
        $createStmt = new CreateStmt();

        $rangeVar = new RangeVar();
        $rangeVar->setRelname($this->table);
        $rangeVar->setInh(true);

        if ($this->schema !== null) {
            $rangeVar->setSchemaname($this->schema);
        }

        if ($this->temporary) {
            $rangeVar->setRelpersistence('t');
        } elseif ($this->unlogged) {
            $rangeVar->setRelpersistence('u');
        } else {
            $rangeVar->setRelpersistence('p');
        }

        $createStmt->setRelation($rangeVar);

        if ($this->columns !== [] || $this->constraints !== []) {
            $tableElts = [];

            foreach ($this->columns as $column) {
                $node = new Node();
                $node->setColumnDef($column->toAst());
                $tableElts[] = $node;
            }

            foreach ($this->constraints as $constraint) {
                $node = new Node();
                $node->setConstraint($constraint->toAst());
                $tableElts[] = $node;
            }

            $createStmt->setTableElts($tableElts);
        }

        if ($this->ifNotExists) {
            $createStmt->setIfNotExists(true);
        }

        if ($this->temporary && $this->onCommitAction !== null) {
            $createStmt->setOncommit($this->onCommitAction);
        }

        if ($this->inherits !== []) {
            $inhRelations = [];

            foreach ($this->inherits as $parentTable) {
                $parentRangeVar = new RangeVar();
                $parentRangeVar->setRelname($parentTable);
                $parentRangeVar->setInh(true);
                $parentRangeVar->setRelpersistence('p');

                $node = new Node();
                $node->setRangeVar($parentRangeVar);
                $inhRelations[] = $node;
            }

            $createStmt->setInhRelations($inhRelations);
        }

        if ($this->partitionStrategy !== null && $this->partitionColumns !== []) {
            $partitionSpec = new PartitionSpec();
            $partitionSpec->setStrategy($this->partitionStrategy);

            $partParams = [];

            foreach ($this->partitionColumns as $columnName) {
                $partitionElem = new PartitionElem();
                $partitionElem->setName($columnName);

                $node = new Node();
                $node->setPartitionElem($partitionElem);
                $partParams[] = $node;
            }

            $partitionSpec->setPartParams($partParams);
            $createStmt->setPartspec($partitionSpec);
        }

        if ($this->tablespace !== null) {
            $createStmt->setTablespacename($this->tablespace);
        }

        return $createStmt;
    }

    public function unlogged() : CreateTableFinalStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->constraints,
            $this->ifNotExists,
            false,
            true,
            $this->inherits,
            $this->partitionStrategy,
            $this->partitionColumns,
            $this->tablespace,
            $this->onCommitAction,
        );
    }
}
