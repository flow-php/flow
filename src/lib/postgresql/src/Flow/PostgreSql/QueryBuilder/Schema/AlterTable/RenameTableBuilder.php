<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\AlterTable;

use Flow\PostgreSql\Protobuf\AST\{ObjectType, RangeVar, RenameStmt};

final readonly class RenameTableBuilder
{
    private function __construct(
        private string $table,
        private ?string $schema,
        private int $renameType,
        private ?string $subname,
        private string $newname,
        private bool $ifExists = false,
    ) {
    }

    public static function renameColumn(string $table, ?string $schema, string $oldName, string $newName, bool $ifExists) : self
    {
        return new self(
            $table,
            $schema,
            ObjectType::OBJECT_COLUMN,
            $oldName,
            $newName,
            $ifExists,
        );
    }

    public static function renameConstraint(string $table, ?string $schema, string $oldName, string $newName, bool $ifExists) : self
    {
        return new self(
            $table,
            $schema,
            ObjectType::OBJECT_TABCONSTRAINT,
            $oldName,
            $newName,
            $ifExists,
        );
    }

    public static function renameTo(string $table, ?string $schema, string $newName, bool $ifExists) : self
    {
        return new self(
            $table,
            $schema,
            ObjectType::OBJECT_TABLE,
            null,
            $newName,
            $ifExists,
        );
    }

    public function toAst() : RenameStmt
    {
        $stmt = new RenameStmt();
        $stmt->setRenameType($this->renameType);

        $rangeVar = new RangeVar();
        $rangeVar->setRelname($this->table);
        $rangeVar->setRelpersistence('p');
        $rangeVar->setInh(true);

        if ($this->schema !== null) {
            $rangeVar->setSchemaname($this->schema);
        }

        $stmt->setRelation($rangeVar);

        if ($this->renameType === ObjectType::OBJECT_COLUMN || $this->renameType === ObjectType::OBJECT_TABCONSTRAINT) {
            $stmt->setRelationType(ObjectType::OBJECT_TABLE);

            if ($this->subname !== null) {
                $stmt->setSubname($this->subname);
            }
        }

        $stmt->setNewname($this->newname);

        if ($this->ifExists) {
            $stmt->setMissingOk(true);
        }

        return $stmt;
    }
}
