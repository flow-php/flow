<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Table;

use Flow\PostgreSql\Protobuf\AST\{Node, PBList, RangeFunction};
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\FunctionCall;

/**
 * Represents a table-valued function: generate_series(1, 10), unnest(array), etc.
 * Supports WITH ORDINALITY to add row numbering.
 */
final readonly class TableFunction implements TableReference
{
    /**
     * @param FunctionCall $function The table-valued function
     * @param bool $withOrdinality Whether to add WITH ORDINALITY
     */
    public function __construct(
        private FunctionCall $function,
        private bool $withOrdinality = false,
    ) {
    }

    public static function fromAst(Node $node) : static
    {
        $rangeFunction = $node->getRangeFunction();

        if ($rangeFunction === null) {
            throw InvalidAstException::unexpectedNodeType('RangeFunction', 'unknown');
        }

        $functions = $rangeFunction->getFunctions();

        if ($functions === null || \count($functions) === 0) {
            throw InvalidAstException::missingRequiredField('functions', 'RangeFunction');
        }

        $firstFunction = $functions[0];
        $list = $firstFunction->getList();

        if ($list === null) {
            throw InvalidAstException::invalidFieldValue('functions', 'RangeFunction', 'expected List node');
        }

        $items = $list->getItems();

        if ($items === null || \count($items) === 0) {
            throw InvalidAstException::invalidFieldValue('functions', 'RangeFunction', 'List cannot be empty');
        }

        $functionNode = $items[0];
        $function = FunctionCall::fromAst($functionNode);

        $withOrdinality = $rangeFunction->getOrdinality();

        return new self($function, $withOrdinality);
    }

    public function as(string $alias, ?array $columnAliases = null) : AliasedTable
    {
        return new AliasedTable($this, $alias, $columnAliases);
    }

    public function getFunction() : FunctionCall
    {
        return $this->function;
    }

    public function isWithOrdinality() : bool
    {
        return $this->withOrdinality;
    }

    public function toAst() : Node
    {
        $functionList = new PBList();
        $functionList->setItems([$this->function->toAst()]);

        $listNode = new Node();
        $listNode->setList($functionList);

        $rangeFunction = new RangeFunction();
        $rangeFunction->setFunctions([$listNode]);
        $rangeFunction->setOrdinality($this->withOrdinality);
        $rangeFunction->setLateral(false);
        $rangeFunction->setIsRowsfrom(false);

        $node = new Node();
        $node->setRangeFunction($rangeFunction);

        return $node;
    }

    public function withOrdinality(bool $withOrdinality = true) : self
    {
        return new self($this->function, $withOrdinality);
    }
}
