<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\View\AlterMaterializedView;

use Flow\PgQuery\Protobuf\AST\{AlterObjectSchemaStmt, ObjectType, RangeVar};
use Flow\PgQuery\QueryBuilder\AstToSql;

final readonly class AlterMatViewSchemaBuilder implements AlterMatViewSchemaFinalStep
{
    use AstToSql;

    private function __construct(
        private string $view,
        private ?string $schema,
        private string $newSchema,
        private bool $ifExists,
    ) {
    }

    public static function create(string $view, ?string $schema, string $newSchema, bool $ifExists) : self
    {
        return new self($view, $schema, $newSchema, $ifExists);
    }

    public function toAst() : AlterObjectSchemaStmt
    {
        $stmt = new AlterObjectSchemaStmt();
        $stmt->setObjectType(ObjectType::OBJECT_MATVIEW);

        $rangeVar = new RangeVar();
        $rangeVar->setRelname($this->view);
        $rangeVar->setInh(true);
        $rangeVar->setRelpersistence('p');

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
