<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Constraint;

use Flow\PostgreSql\Protobuf\AST\Constraint;
use Flow\PostgreSql\Protobuf\AST\ConstrType;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\Protobuf\AST\RangeVar;
use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;
use Flow\PostgreSql\QueryBuilder\Schema\ReferentialAction;

final readonly class ForeignKeyConstraint implements TableConstraint
{
    /**
     * @param list<string> $columns
     * @param list<string> $referenceColumns
     */
    private function __construct(
        private array $columns,
        private string $referenceTable,
        private array $referenceColumns,
        private ?string $referenceSchema = null,
        private ?string $name = null,
        private ?ReferentialAction $onUpdate = null,
        private ?ReferentialAction $onDelete = null,
        private bool $deferrable = false,
        private bool $initiallyDeferred = false,
    ) {}

    /**
     * @param list<string> $columns
     * @param list<string> $referenceColumns
     */
    public static function create(array $columns, string $referenceTable, array $referenceColumns = []): self
    {
        $identifier = QualifiedIdentifier::parse($referenceTable);

        return new self($columns, $identifier->name(), $referenceColumns, $identifier->schema());
    }

    public function deferrable(bool $initiallyDeferred = false): self
    {
        return new self(
            $this->columns,
            $this->referenceTable,
            $this->referenceColumns,
            $this->referenceSchema,
            $this->name,
            $this->onUpdate,
            $this->onDelete,
            true,
            $initiallyDeferred,
        );
    }

    public function name(string $name): self
    {
        return new self(
            $this->columns,
            $this->referenceTable,
            $this->referenceColumns,
            $this->referenceSchema,
            $name,
            $this->onUpdate,
            $this->onDelete,
            $this->deferrable,
            $this->initiallyDeferred,
        );
    }

    public function onDelete(ReferentialAction $action): self
    {
        return new self(
            $this->columns,
            $this->referenceTable,
            $this->referenceColumns,
            $this->referenceSchema,
            $this->name,
            $this->onUpdate,
            $action,
            $this->deferrable,
            $this->initiallyDeferred,
        );
    }

    public function onUpdate(ReferentialAction $action): self
    {
        return new self(
            $this->columns,
            $this->referenceTable,
            $this->referenceColumns,
            $this->referenceSchema,
            $this->name,
            $action,
            $this->onDelete,
            $this->deferrable,
            $this->initiallyDeferred,
        );
    }

    public function schema(string $schema): self
    {
        return new self(
            $this->columns,
            $this->referenceTable,
            $this->referenceColumns,
            $schema,
            $this->name,
            $this->onUpdate,
            $this->onDelete,
            $this->deferrable,
            $this->initiallyDeferred,
        );
    }

    public function toAst(): Constraint
    {
        $constraint = new Constraint();
        $constraint->setContype(ConstrType::CONSTR_FOREIGN);

        if ($this->name !== null) {
            $constraint->setConname($this->name);
        }

        $fkAttrs = [];

        foreach ($this->columns as $column) {
            $fkAttrs[] = $this->createStringNode($column);
        }

        $constraint->setFkAttrs($fkAttrs);

        $rangeVar = new RangeVar();
        $rangeVar->setRelname($this->referenceTable);
        $rangeVar->setInh(true);

        if ($this->referenceSchema !== null) {
            $rangeVar->setSchemaname($this->referenceSchema);
        }

        $constraint->setPktable($rangeVar);

        if ($this->referenceColumns !== []) {
            $pkAttrs = [];

            foreach ($this->referenceColumns as $column) {
                $pkAttrs[] = $this->createStringNode($column);
            }

            $constraint->setPkAttrs($pkAttrs);
        }

        if ($this->onUpdate !== null) {
            $constraint->setFkUpdAction($this->onUpdate->value);
        }

        if ($this->onDelete !== null) {
            $constraint->setFkDelAction($this->onDelete->value);
        }

        if ($this->deferrable) {
            $constraint->setDeferrable(true);

            if ($this->initiallyDeferred) {
                $constraint->setInitdeferred(true);
            }
        }

        return $constraint;
    }

    private function createStringNode(string $value): Node
    {
        $str = new PBString();
        $str->setSval($value);

        $node = new Node();
        $node->setString($str);

        return $node;
    }
}
