<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\View\AlterMaterializedView;

use Flow\PgQuery\Protobuf\AST\{DropBehavior, ObjectType, RangeVar, RenameStmt};

final readonly class RenameMatViewBuilder implements RenameMatViewFinalStep
{
    private function __construct(
        private string $view,
        private ?string $schema,
        private string $newName,
        private bool $ifExists,
    ) {
    }

    public static function create(string $view, ?string $schema, string $newName, bool $ifExists) : self
    {
        return new self($view, $schema, $newName, $ifExists);
    }

    public function toAst() : RenameStmt
    {
        $stmt = new RenameStmt();
        $stmt->setRenameType(ObjectType::OBJECT_MATVIEW);

        $rangeVar = new RangeVar();
        $rangeVar->setRelname($this->view);
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
