<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Type;

use Flow\PgQuery\Protobuf\AST\{AlterEnumStmt, Node, PBString};

final readonly class AlterEnumTypeBuilder implements AlterEnumTypeActionStep, AlterEnumTypeFinalStep
{
    private function __construct(
        private string $name,
        private ?string $schema = null,
        private ?string $oldVal = null,
        private ?string $newVal = null,
        private ?string $neighbor = null,
        private bool $isAfter = false,
        private bool $skipIfExists = false,
    ) {
    }

    public static function create(string $name) : AlterEnumTypeActionStep
    {
        $parts = \explode('.', $name);

        if (\count($parts) === 2) {
            return new self($parts[1], $parts[0]);
        }

        return new self($name);
    }

    public function addValue(string $value) : AlterEnumTypeFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            null,
            $value,
            null,
            false,
            $this->skipIfExists,
        );
    }

    public function addValueAfter(string $value, string $neighbor) : AlterEnumTypeFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            null,
            $value,
            $neighbor,
            true,
            $this->skipIfExists,
        );
    }

    public function addValueBefore(string $value, string $neighbor) : AlterEnumTypeFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            null,
            $value,
            $neighbor,
            false,
            $this->skipIfExists,
        );
    }

    public function ifNotExists() : AlterEnumTypeFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            $this->oldVal,
            $this->newVal,
            $this->neighbor,
            $this->isAfter,
            true,
        );
    }

    public function renameValue(string $oldValue, string $newValue) : AlterEnumTypeFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            $oldValue,
            $newValue,
            null,
            false,
            false,
        );
    }

    public function toAst() : AlterEnumStmt
    {
        $stmt = new AlterEnumStmt();

        $typeNameNodes = [];

        if ($this->schema !== null) {
            $str = new PBString();
            $str->setSval($this->schema);
            $node = new Node();
            $node->setString($str);
            $typeNameNodes[] = $node;
        }

        $str = new PBString();
        $str->setSval($this->name);
        $node = new Node();
        $node->setString($str);
        $typeNameNodes[] = $node;

        $stmt->setTypeName($typeNameNodes);

        if ($this->oldVal !== null) {
            $stmt->setOldVal($this->oldVal);
        }

        if ($this->newVal !== null) {
            $stmt->setNewVal($this->newVal);
        }

        if ($this->neighbor !== null) {
            $stmt->setNewValNeighbor($this->neighbor);
            $stmt->setNewValIsAfter($this->isAfter);
        }

        $stmt->setSkipIfNewValExists($this->skipIfExists);

        return $stmt;
    }
}
