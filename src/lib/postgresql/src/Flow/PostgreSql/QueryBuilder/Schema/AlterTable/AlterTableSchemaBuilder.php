<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\AlterTable;

use Flow\PostgreSql\Protobuf\AST\{AlterObjectSchemaStmt, ObjectType, RangeVar};

final readonly class AlterTableSchemaBuilder
{
    private function __construct(
        private string $table,
        private ?string $schema,
        private string $newSchema,
        private bool $ifExists = false,
    ) {
    }

    public static function create(string $table, ?string $schema, string $newSchema, bool $ifExists) : self
    {
        return new self($table, $schema, $newSchema, $ifExists);
    }

    public function toAst() : AlterObjectSchemaStmt
    {
        $stmt = new AlterObjectSchemaStmt();
        $stmt->setObjectType(ObjectType::OBJECT_TABLE);

        $rangeVar = new RangeVar();
        $rangeVar->setRelname($this->table);
        $rangeVar->setRelpersistence('p');
        $rangeVar->setInh(true);

        if ($this->schema !== null) {
            $rangeVar->setSchemaname($this->schema);
        }

        $stmt->setRelation($rangeVar);
        $stmt->setNewschema($this->newSchema);

        if ($this->ifExists) {
            $stmt->setMissingOk(true);
        }

        return $stmt;
    }
}
