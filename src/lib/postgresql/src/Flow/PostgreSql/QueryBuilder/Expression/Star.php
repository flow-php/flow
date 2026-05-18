<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\A_Star;
use Flow\PostgreSql\Protobuf\AST\ColumnRef;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;

use function count;

/**
 * Represents a star expression in SQL (e.g., "*" or "table.*").
 */
final readonly class Star implements Expression
{
    private function __construct(
        private ?string $table = null,
    ) {}

    /**
     * Create a plain star: SELECT *.
     */
    public static function all(): self
    {
        return new self();
    }

    public static function fromAst(Node $node): static
    {
        $aStar = $node->getAStar();

        if ($aStar !== null) {
            return new self();
        }

        $columnRef = $node->getColumnRef();

        if ($columnRef !== null) {
            $fields = $columnRef->getFields();

            if (count($fields) === 0) {
                throw InvalidAstException::missingRequiredField('fields', 'ColumnRef');
            }

            $lastField = $fields[count($fields) - 1];
            $aStar = $lastField->getAStar();

            if ($aStar === null) {
                throw InvalidAstException::unexpectedNodeType('A_Star', 'unknown');
            }

            if (count($fields) === 1) {
                return new self();
            }

            if (count($fields) === 2) {
                $tableField = $fields[0];
                $tableString = $tableField->getString();

                if ($tableString === null) {
                    throw InvalidAstException::invalidFieldValue('fields[0]', 'ColumnRef', 'Expected String node');
                }

                return new self($tableString->getSval());
            }

            throw InvalidAstException::invalidFieldValue('fields', 'ColumnRef', 'Star can only have table prefix');
        }

        throw InvalidAstException::unexpectedNodeType('A_Star or ColumnRef', 'unknown');
    }

    /**
     * Create a qualified star: SELECT table.*.
     */
    public static function fromTable(string $table): self
    {
        return new self($table);
    }

    public function as(string $alias): AliasedExpression
    {
        return AliasedExpression::create($this, $alias);
    }

    public function isQualified(): bool
    {
        return $this->table !== null;
    }

    public function table(): ?string
    {
        return $this->table;
    }

    public function toAst(): Node
    {
        $columnRef = new ColumnRef();
        $fields = [];

        if ($this->table !== null) {
            $tableString = new PBString();
            $tableString->setSval($this->table);
            $tableNode = new Node();
            $tableNode->setString($tableString);
            $fields[] = $tableNode;
        }

        $aStar = new A_Star();
        $starNode = new Node();
        $starNode->setAStar($aStar);
        $fields[] = $starNode;

        $columnRef->setFields($fields);

        $node = new Node();
        $node->setColumnRef($columnRef);

        return $node;
    }
}
