<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\Function\ExpandingFunctions;
use Flow\ETL\Function\FunctionTree;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\ScalarFunction\ExpandResults;
use Flow\ETL\Row;
use Flow\ETL\Row\ResolvedReference;
use Flow\ETL\Schema;
use Flow\Types\Type;
use Flow\Types\Type\TypeWidener;

use function array_key_exists;
use function array_map;
use function array_values;
use function count;
use function max;
use function spl_object_id;

final readonly class NestedExpansion
{
    /**
     * @param non-empty-array<string, ExpandResults> $expands keyed by the synthesized column the root reads
     *                                                        each element from
     */
    private function __construct(
        private ScalarFunction $root,
        private array $expands,
    ) {}

    public static function of(ScalarFunction $resolved, Schema $input): ?self
    {
        // a root expand runs in ScalarFunctionTransformer's own loop
        if ($resolved instanceof ExpandResults) {
            return null;
        }

        $distinct = [];

        foreach ((new ExpandingFunctions())->in($resolved) as $expand) {
            $distinct[spl_object_id($expand)] = $expand;
        }

        if ($distinct === []) {
            return null;
        }

        // two or more expands zip to the longest, so each of them can be padded with null
        $pads = count($distinct) > 1;
        $references = [];
        $expands = [];
        $suffix = 0;

        foreach ($distinct as $id => $expand) {
            do {
                $name = "\0expand:" . $suffix++;
            } while ($input->findDefinition($name) !== null);

            $references[$id] = new ResolvedReference(
                $name,
                $pads ? (new TypeWidener())->nullable($expand->returns()) : $expand->returns(),
            );
            $expands[$name] = $expand;
        }

        /** @var ScalarFunction $root the root is not an expand, so rewrite() rebuilds it with its own class */
        $root = self::rewrite($resolved, $references);

        return new self($root, $expands);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return $this->root->returns();
    }

    /**
     * @return list<mixed>
     */
    public function eval(Row $row, FlowContext $context): array
    {
        $lists = [];

        foreach ($this->expands as $name => $expand) {
            $lists[$name] = array_values($expand->eval($row, $context));
        }

        $values = [];
        $length = max(array_map(count(...), $lists));
        $element = $row->values();

        for ($position = 0; $position < $length; $position++) {
            foreach ($lists as $name => $list) {
                $element[$name] = $list[$position] ?? null;
            }

            $values[] = $this->root->eval(new Row($element), $context);
        }

        return $values;
    }

    /**
     * @param array<int, ResolvedReference> $references keyed by spl_object_id() of the expand each replaces
     */
    private static function rewrite(FunctionTree $node, array $references): FunctionTree
    {
        if (array_key_exists(spl_object_id($node), $references)) {
            return $references[spl_object_id($node)];
        }

        $children = $node->children();
        $rewritten = array_map(static fn(FunctionTree $child): FunctionTree => self::rewrite(
            $child,
            $references,
        ), $children);

        return $rewritten === $children ? $node : $node->withChildren($rewritten);
    }
}
