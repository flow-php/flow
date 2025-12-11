<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Trigger;

use Flow\PostgreSql\Protobuf\AST\{AlterObjectDependsStmt, Node, ObjectType, PBList, PBString, RangeVar, RenameStmt};
use Flow\PostgreSql\QueryBuilder\{AstToSql, QualifiedIdentifier};
use Flow\PostgreSql\QueryBuilder\Exception\InvalidBuilderStateException;

final readonly class AlterTriggerBuilder implements AlterTriggerActionStep, AlterTriggerFinalStep, AlterTriggerOnStep
{
    use AstToSql;

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
        if ($schema !== null) {
            return new self(
                $this->name,
                $table,
                $schema,
                $this->newName,
                $this->extension,
                $this->removeDepends,
            );
        }

        $identifier = QualifiedIdentifier::parse($table);

        return new self(
            $this->name,
            $identifier->name(),
            $identifier->schema(),
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

    public function toAst() : RenameStmt|AlterObjectDependsStmt
    {
        if ($this->newName !== null && $this->extension !== null) {
            throw InvalidBuilderStateException::mutuallyExclusiveOptions('RENAME TO', 'DEPENDS ON EXTENSION');
        }

        if ($this->newName !== null) {
            return $this->buildRenameAst();
        }

        return $this->buildDependsAst();
    }

    private function buildDependsAst() : AlterObjectDependsStmt
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

    private function buildRenameAst() : RenameStmt
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
