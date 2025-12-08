<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Trigger;

use Flow\PgQuery\Protobuf\AST\{AlterObjectDependsStmt, Node, ObjectType, PBList, PBString, RangeVar, RenameStmt};

final readonly class AlterTriggerBuilder implements AlterTriggerActionStep, AlterTriggerFinalStep, AlterTriggerOnStep
{
    private function __construct(
        private string $name,
        private ?string $table = null,
        private ?string $schema = null,
        private ?string $newName = null,
        private ?string $extension = null,
        private bool $removeDepends = false,
    ) {
    }

    public static function create(string $name) : AlterTriggerOnStep
    {
        return new self($name);
    }

    public function dependsOnExtension(string $extension) : AlterTriggerFinalStep
    {
        return new self(
            $this->name,
            $this->table,
            $this->schema,
            null,
            $extension,
            false,
        );
    }

    public function noDependsOnExtension(string $extension) : AlterTriggerFinalStep
    {
        return new self(
            $this->name,
            $this->table,
            $this->schema,
            null,
            $extension,
            true,
        );
    }

    public function on(string $table, ?string $schema = null) : AlterTriggerActionStep
    {
        $parts = \explode('.', $table);

        if (\count($parts) === 2) {
            return new self(
                $this->name,
                $parts[1],
                $parts[0],
                $this->newName,
                $this->extension,
                $this->removeDepends,
            );
        }

        return new self(
            $this->name,
            $table,
            $schema,
            $this->newName,
            $this->extension,
            $this->removeDepends,
        );
    }

    public function renameTo(string $newName) : AlterTriggerFinalStep
    {
        return new self(
            $this->name,
            $this->table,
            $this->schema,
            $newName,
            null,
            false,
        );
    }

    public function toDependsAst() : AlterObjectDependsStmt
    {
        $stmt = new AlterObjectDependsStmt();

        $stmt->setObjectType(ObjectType::OBJECT_TRIGGER);
        $stmt->setRemove($this->removeDepends);

        if ($this->table !== null) {
            $rangeVar = new RangeVar();
            $rangeVar->setRelname($this->table);
            $rangeVar->setRelpersistence('p');
            $rangeVar->setInh(true);

            if ($this->schema !== null) {
                $rangeVar->setSchemaname($this->schema);
            }
            $stmt->setRelation($rangeVar);
        }

        $str = new PBString();
        $str->setSval($this->name);
        $itemNode = new Node();
        $itemNode->setString($str);

        $list = new PBList();
        $list->setItems([$itemNode]);

        $objectNode = new Node();
        $objectNode->setList($list);
        $stmt->setObject($objectNode);

        if ($this->extension !== null) {
            $extStr = new PBString();
            $extStr->setSval($this->extension);
            $stmt->setExtname($extStr);
        }

        return $stmt;
    }

    public function toRenameAst() : RenameStmt
    {
        $stmt = new RenameStmt();

        $stmt->setRenameType(ObjectType::OBJECT_TRIGGER);
        $stmt->setSubname($this->name);

        if ($this->newName !== null) {
            $stmt->setNewname($this->newName);
        }

        if ($this->table !== null) {
            $rangeVar = new RangeVar();
            $rangeVar->setRelname($this->table);
            $rangeVar->setRelpersistence('p');
            $rangeVar->setInh(true);

            if ($this->schema !== null) {
                $rangeVar->setSchemaname($this->schema);
            }
            $stmt->setRelation($rangeVar);
        }

        return $stmt;
    }
}
