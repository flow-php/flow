<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\CommonTableExpr;
use Flow\PostgreSql\Protobuf\AST\CTECycleClause;
use Flow\PostgreSql\Protobuf\AST\CTESearchClause;
use Flow\PostgreSql\Protobuf\AST\DeleteStmt;
use Flow\PostgreSql\Protobuf\AST\InsertStmt;
use Flow\PostgreSql\Protobuf\AST\MergeStmt;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use Flow\PostgreSql\Protobuf\AST\UpdateStmt;
use Flow\PostgreSql\QueryBuilder\Bridge\AstConvertible;
use Flow\PostgreSql\QueryBuilder\Delete\DeleteFinalStep;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;
use Flow\PostgreSql\QueryBuilder\Expression\Literal;
use Flow\PostgreSql\QueryBuilder\Insert\InsertFinalStep;
use Flow\PostgreSql\QueryBuilder\Merge\MergeFinalStep;
use Flow\PostgreSql\QueryBuilder\Select\SelectFinalStep;
use Flow\PostgreSql\QueryBuilder\Update\UpdateFinalStep;
use InvalidArgumentException;

use function array_map;
use function array_values;

/**
 * Represents a Common Table Expression (CTE).
 */
final readonly class CTE implements AstConvertible
{
    private Node $query;

    /**
     * @param array<string> $columnNames
     */
    public function __construct(
        private string $name,
        SelectFinalStep|InsertFinalStep|UpdateFinalStep|DeleteFinalStep|MergeFinalStep|Node $query,
        private array $columnNames = [],
        private CTEMaterialization $materialization = CTEMaterialization::DEFAULT,
        private ?CTESearchClause $search = null,
        private ?CTECycleClause $cycle = null,
    ) {
        if ($query instanceof Node) {
            $this->query = $query;

            return;
        }

        $statement = $query->toAst();
        $node = new Node();

        $this->query = match (true) {
            $statement instanceof SelectStmt => $node->setSelectStmt($statement),
            $statement instanceof InsertStmt => $node->setInsertStmt($statement),
            $statement instanceof UpdateStmt => $node->setUpdateStmt($statement),
            $statement instanceof DeleteStmt => $node->setDeleteStmt($statement),
            $statement instanceof MergeStmt => $node->setMergeStmt($statement),
        };
    }

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

        return new self(
            $cteName,
            $cteQuery,
            $columnNames,
            CTEMaterialization::fromProtobuf($commonTableExpr->getCtematerialized()),
            $commonTableExpr->getSearchClause(),
            $commonTableExpr->getCycleClause(),
        );
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

    /**
     * @param list<string> $columns
     */
    public function cycle(
        array $columns,
        string $markColumn,
        string $pathColumn,
        ?Expression $markValue = null,
        ?Expression $markDefault = null,
    ): self {
        if ($columns === []) {
            throw new InvalidArgumentException('CYCLE needs at least one column');
        }

        if (($markValue === null) !== ($markDefault === null)) {
            throw new InvalidArgumentException('CYCLE needs both a mark value and a mark default, or neither');
        }

        $cycle = new CTECycleClause();
        $cycle->setCycleColList(self::stringNodes($columns));
        $cycle->setCycleMarkColumn($markColumn);
        $cycle->setCyclePathColumn($pathColumn);
        $cycle->setCycleMarkValue(($markValue ?? Literal::bool(true))->toAst());
        $cycle->setCycleMarkDefault(($markDefault ?? Literal::bool(false))->toAst());

        return new self($this->name, $this->query, $this->columnNames, $this->materialization, $this->search, $cycle);
    }

    public function materialized(): self
    {
        return new self(
            $this->name,
            $this->query,
            $this->columnNames,
            CTEMaterialization::MATERIALIZED,
            $this->search,
            $this->cycle,
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
            $this->search,
            $this->cycle,
        );
    }

    public function query(): Node
    {
        return $this->query;
    }

    /**
     * @param list<string> $columns
     */
    public function searchBreadthFirst(array $columns, string $sequenceColumn): self
    {
        return $this->search($columns, $sequenceColumn, true);
    }

    /**
     * @param list<string> $columns
     */
    public function searchDepthFirst(array $columns, string $sequenceColumn): self
    {
        return $this->search($columns, $sequenceColumn, false);
    }

    public function toAst(): Node
    {
        $commonTableExpr = new CommonTableExpr();
        $commonTableExpr->setCtename($this->name);
        $commonTableExpr->setCtequery($this->query);
        $commonTableExpr->setCtematerialized($this->materialization->toProtobuf());
        $commonTableExpr->setSearchClause($this->search);
        $commonTableExpr->setCycleClause($this->cycle);

        if ($this->columnNames !== []) {
            $commonTableExpr->setAliascolnames(self::stringNodes($this->columnNames));
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
        return new self($this->name, $this->query, $columns, $this->materialization, $this->search, $this->cycle);
    }

    /**
     * @param array<string> $names
     *
     * @return list<Node>
     */
    private static function stringNodes(array $names): array
    {
        return array_values(array_map(
            static fn(string $name): Node => (new Node())->setString((new PBString())->setSval($name)),
            $names,
        ));
    }

    /**
     * @param list<string> $columns
     */
    private function search(array $columns, string $sequenceColumn, bool $breadthFirst): self
    {
        if ($columns === []) {
            throw new InvalidArgumentException('SEARCH needs at least one column');
        }

        $search = new CTESearchClause();
        $search->setSearchColList(self::stringNodes($columns));
        $search->setSearchBreadthFirst($breadthFirst);
        $search->setSearchSeqColumn($sequenceColumn);

        return new self($this->name, $this->query, $this->columnNames, $this->materialization, $search, $this->cycle);
    }
}
