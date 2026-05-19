<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\CommonTableExpr;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\QueryBuilder\Bridge\AstConvertible;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;

/**
 * Represents a Common Table Expression (CTE).
 */
final readonly class CTE implements AstConvertible
{
    /**
     * @param array<string> $columnNames
     */
    public function __construct(
        private string $name,
        private Node $query,
        private array $columnNames = [],
        private CTEMaterialization $materialization = CTEMaterialization::DEFAULT,
        private bool $recursive = false,
    ) {}

    public static function fromAst(Node $node): static
    {
        $commonTableExpr = $node->getCommonTableExpr();

        if ($commonTableExpr === null) {
            throw InvalidAstException::unexpectedNodeType('CommonTableExpr', 'unknown');
        }

        $cteName = $commonTableExpr->getCtename();

        if ($cteName === '') {
            throw InvalidAstException::missingRequiredField('ctename', 'CommonTableExpr');
        }

        $cteQuery = $commonTableExpr->getCtequery();

        if ($cteQuery === null) {
            throw InvalidAstException::missingRequiredField('ctequery', 'CommonTableExpr');
        }

        $columnNames = [];

        foreach ($commonTableExpr->getAliascolnames() as $aliasNode) {
            $stringNode = $aliasNode->getString();

            if ($stringNode === null) {
                throw InvalidAstException::invalidFieldValue(
                    'aliascolnames',
                    'CommonTableExpr',
                    'Expected String node',
                );
            }

            $columnNames[] = $stringNode->getSval();
        }

        $materialization = CTEMaterialization::fromProtobuf($commonTableExpr->getCtematerialized());
        $recursive = $commonTableExpr->getCterecursive();

        return new self($cteName, $cteQuery, $columnNames, $materialization, $recursive);
    }

    /**
     * @return array<string>
     */
    public function columnNames(): array
    {
        return $this->columnNames;
    }

    public function materialization(): CTEMaterialization
    {
        return $this->materialization;
    }

    public function materialized(): self
    {
        return new self(
            $this->name,
            $this->query,
            $this->columnNames,
            CTEMaterialization::MATERIALIZED,
            $this->recursive,
        );
    }

    public function name(): string
    {
        return $this->name;
    }

    public function notMaterialized(): self
    {
        return new self(
            $this->name,
            $this->query,
            $this->columnNames,
            CTEMaterialization::NOT_MATERIALIZED,
            $this->recursive,
        );
    }

    public function query(): Node
    {
        return $this->query;
    }

    public function recursive(): bool
    {
        return $this->recursive;
    }

    public function toAst(): Node
    {
        $commonTableExpr = new CommonTableExpr();
        $commonTableExpr->setCtename($this->name);
        $commonTableExpr->setCtequery($this->query);
        $commonTableExpr->setCtematerialized($this->materialization->toProtobuf());
        $commonTableExpr->setCterecursive($this->recursive);

        if ($this->columnNames !== []) {
            $aliasNodes = [];

            foreach ($this->columnNames as $columnName) {
                $stringNode = new PBString();
                $stringNode->setSval($columnName);

                $node = new Node();
                $node->setString($stringNode);

                $aliasNodes[] = $node;
            }

            $commonTableExpr->setAliascolnames($aliasNodes);
        }

        $node = new Node();
        $node->setCommonTableExpr($commonTableExpr);

        return $node;
    }

    /**
     * @param array<string> $columns
     */
    public function withColumns(array $columns): self
    {
        return new self($this->name, $this->query, $columns, $this->materialization, $this->recursive);
    }
}
