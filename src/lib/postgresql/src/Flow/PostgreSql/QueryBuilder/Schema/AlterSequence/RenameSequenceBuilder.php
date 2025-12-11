<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\AlterSequence;

use Flow\PostgreSql\Protobuf\AST\{DropBehavior, ObjectType, RangeVar, RenameStmt};
use Flow\PostgreSql\QueryBuilder\AstToSql;

final readonly class RenameSequenceBuilder implements RenameSequenceFinalStep
{
    use AstToSql;

    private function __construct(
        private string $sequence,
        private ?string $schema,
        private string $newName,
        private bool $ifExists,
    ) {
    }

    public static function create(string $sequence, ?string $schema, string $newName, bool $ifExists) : self
    {
        return new self($sequence, $schema, $newName, $ifExists);
    }

    public function toAst() : RenameStmt
    {
        $stmt = new RenameStmt();
        $stmt->setRenameType(ObjectType::OBJECT_SEQUENCE);

        $rangeVar = new RangeVar();
        $rangeVar->setRelname($this->sequence);
        $rangeVar->setInh(true);
        $rangeVar->setRelpersistence('p');

        if ($this->schema !== null) {
            $rangeVar->setSchemaname($this->schema);
        }

        $stmt->setRelation($rangeVar);
        $stmt->setNewname($this->newName);
        $stmt->setBehavior(DropBehavior::DROP_RESTRICT);

        if ($this->ifExists) {
            $stmt->setMissingOk(true);
        }

        return $stmt;
    }
}
