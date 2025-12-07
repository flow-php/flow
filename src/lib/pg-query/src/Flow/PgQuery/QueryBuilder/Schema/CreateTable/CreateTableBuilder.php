<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\CreateTable;

use Flow\PgQuery\Protobuf\AST\{CreateStmt, Node, OnCommitAction, PartitionElem, PartitionSpec, PartitionStrategy, RangeVar};
use Flow\PgQuery\QueryBuilder\Schema\ColumnDefinition;
use Flow\PgQuery\QueryBuilder\Schema\Constraint\TableConstraint;

final readonly class CreateTableBuilder implements CreateTableColumnsStep
{
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
    ) {
    }

    public static function create(string $table, ?string $schema = null) : CreateTableColumnsStep
    {
        return new self($table, $schema);
    }

    public function column(ColumnDefinition $column) : CreateTableColumnsStep
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
        );
    }

    public function temporary() : CreateTableFinalStep
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

        if ($this->temporary) {
            $createStmt->setOncommit(OnCommitAction::ONCOMMIT_DROP);
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
        );
    }
}
