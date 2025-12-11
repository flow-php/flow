<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\{ColumnRef, Node, PBString};
use Flow\PostgreSql\QueryBuilder\Exception\{InvalidAstException, InvalidExpressionException};

/**
 * Represents a column reference in SQL (e.g., "name", "table.column", "schema.table.column").
 */
final readonly class Column implements Expression
{
    /**
     * @param list<string> $parts
     */
    private function __construct(
        private array $parts,
    ) {
        if ($parts === []) {
            throw InvalidExpressionException::emptyArray('Column parts');
        }

        foreach ($parts as $part) {
            if ($part === '') {
                throw InvalidExpressionException::invalidValue('Column part', $part);
            }
        }
    }

    public static function fromAst(Node $node) : static
    {
        $columnRef = $node->getColumnRef();

        if ($columnRef === null) {
            throw InvalidAstException::unexpectedNodeType('ColumnRef', 'unknown');
        }

        $fields = $columnRef->getFields();

        if ($fields === null || \count($fields) === 0) {
            throw InvalidAstException::missingRequiredField('fields', 'ColumnRef');
        }

        $parts = [];

        foreach ($fields as $field) {
            $stringNode = $field->getString();

            if ($stringNode === null) {
                throw InvalidAstException::invalidFieldValue('fields', 'ColumnRef', 'Expected String node');
            }

            $parts[] = $stringNode->getSval();
        }

        return new self($parts);
    }

    /**
     * @param list<string> $parts
     */
    public static function fromParts(array $parts) : self
    {
        return new self($parts);
    }

    /**
     * Create a column reference from a simple column name.
     */
    public static function name(string $name) : self
    {
        return new self([$name]);
    }

    /**
     * Create a column reference from schema, table, and column name.
     */
    public static function schemaTableColumn(string $schema, string $table, string $column) : self
    {
        return new self([$schema, $table, $column]);
    }

    /**
     * Create a column reference from table and column name.
     */
    public static function tableColumn(string $table, string $column) : self
    {
        return new self([$table, $column]);
    }

    public function as(string $alias) : AliasedExpression
    {
        return AliasedExpression::create($this, $alias);
    }

    public function columnName() : string
    {
        return $this->parts[\count($this->parts) - 1];
    }

    /**
     * @return list<string>
     */
    public function parts() : array
    {
        return $this->parts;
    }

    public function schemaName() : ?string
    {
        if (\count($this->parts) < 3) {
            return null;
        }

        return $this->parts[0];
    }

    public function tableName() : ?string
    {
        if (\count($this->parts) < 2) {
            return null;
        }

        return $this->parts[\count($this->parts) - 2];
    }

    public function toAst() : Node
    {
        $columnRef = new ColumnRef();
        $fields = [];

        foreach ($this->parts as $part) {
            $stringNode = new PBString();
            $stringNode->setSval($part);

            $node = new Node();
            $node->setString($stringNode);

            $fields[] = $node;
        }

        $columnRef->setFields($fields);

        $node = new Node();
        $node->setColumnRef($columnRef);

        return $node;
    }
}
