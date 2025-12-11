<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Table;

use Flow\PostgreSql\Protobuf\AST\{Node, RangeVar};
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;

/**
 * Represents a simple table reference: schema.table or just table.
 */
final readonly class Table implements TableReference
{
    public function __construct(
        public string $name,
        public ?string $schema = null,
        public bool $inherits = true,
    ) {
    }

    public static function fromAst(Node $node) : static
    {
        $rangeVar = $node->getRangeVar();

        if ($rangeVar === null) {
            throw InvalidAstException::unexpectedNodeType('RangeVar', 'unknown');
        }

        $relname = $rangeVar->getRelname();

        if ($relname === '') {
            throw InvalidAstException::missingRequiredField('relname', 'RangeVar');
        }

        $schemaname = $rangeVar->getSchemaname();
        $inh = $rangeVar->getInh();

        return new self(
            $relname,
            $schemaname !== '' ? $schemaname : null,
            $inh
        );
    }

    public function as(string $alias, ?array $columnAliases = null) : AliasedTable
    {
        return new AliasedTable($this, $alias, $columnAliases);
    }

    public function toAst() : Node
    {
        $rangeVar = new RangeVar([
            'relname' => $this->name,
            'inh' => $this->inherits,
        ]);

        if ($this->schema !== null) {
            $rangeVar->setSchemaname($this->schema);
        }

        return new Node(['range_var' => $rangeVar]);
    }

    public function withName(string $name) : self
    {
        return new self($name, $this->schema, $this->inherits);
    }

    public function withSchema(?string $schema) : self
    {
        return new self($this->name, $schema, $this->inherits);
    }
}
