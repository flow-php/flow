<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\AlterSequence;

use Flow\PgQuery\Protobuf\AST\{AlterObjectSchemaStmt, ObjectType, RangeVar};
use Flow\PgQuery\QueryBuilder\AstToSql;

final readonly class AlterSequenceSchemaBuilder implements AlterSequenceSchemaFinalStep
{
    use AstToSql;

    private function __construct(
        private string $sequence,
        private ?string $schema,
        private string $newSchema,
        private bool $ifExists,
    ) {
    }

    public static function create(string $sequence, ?string $schema, string $newSchema, bool $ifExists) : self
    {
        return new self($sequence, $schema, $newSchema, $ifExists);
    }

    public function toAst() : AlterObjectSchemaStmt
    {
        $stmt = new AlterObjectSchemaStmt();
        $stmt->setObjectType(ObjectType::OBJECT_SEQUENCE);

        $rangeVar = new RangeVar();
        $rangeVar->setRelname($this->sequence);
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
