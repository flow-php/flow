<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Index\AlterIndex;

use Flow\PgQuery\Protobuf\AST\{ObjectType, RangeVar, RenameStmt};
use Flow\PgQuery\QueryBuilder\AstToSql;

final readonly class RenameIndexBuilder implements RenameIndexFinalStep
{
    use AstToSql;

    private function __construct(
        private string $index,
        private ?string $schema,
        private string $newName,
        private bool $ifExists,
    ) {
    }

    public static function create(string $index, ?string $schema, string $newName, bool $ifExists) : RenameIndexFinalStep
    {
        return new self($index, $schema, $newName, $ifExists);
    }

    public function toAst() : RenameStmt
    {
        $stmt = new RenameStmt();
        $stmt->setRenameType(ObjectType::OBJECT_INDEX);

        $rangeVar = new RangeVar();
        $rangeVar->setRelname($this->index);
        $rangeVar->setInh(true);
        $rangeVar->setRelpersistence('p');

        if ($this->schema !== null) {
            $rangeVar->setSchemaname($this->schema);
        }

        $stmt->setRelation($rangeVar);
        $stmt->setNewname($this->newName);

        if ($this->ifExists) {
            $stmt->setMissingOk(true);
        }

        return $stmt;
    }
}
