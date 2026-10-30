<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Transformer;

use Flow\ETL\Functions\ExpandingFunctions;
use Flow\ETL\Functions\FunctionTree;
use Flow\ETL\Functions\Parameter;
use Flow\ETL\Functions\Reference\ResolvedReference;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Functions\ScalarFunction\ExpandResults;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Definitions;
use Flow\Types\Type;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\TypeWidener;

use function array_key_exists;
use function array_map;
use function array_slice;
use function array_values;
use function count;
use function is_array;
use function max;
use function spl_object_id;

final readonly class Expansion
{
    /**
     * @param non-empty-array<string, ExpandResults> $expands keyed by the synthesized column the root reads
     *                                                        each element from
     * @param array<string, Definition<mixed>> $expandDefinitions the synthesized columns, keyed like $expands
     */
    private function __construct(
        private ScalarFunction $root,
        private array $expands,
        private array $expandDefinitions,
    ) {}

    /**
     * Null only when the tree holds no expand. A root expand is rewritten too: its root becomes the synthesized
     * reference, so a root, a nested and an unpacked expand are one shape.
     */
    public static function of(ScalarFunction $resolved, Schema $input): ?self
    {
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
        $expandDefinitions = [];
        $suffix = 0;

        foreach ($distinct as $id => $expand) {
            do {
                $name = "\0expand:" . $suffix++;
            } while ($input->findDefinition($name) !== null);

            $type = $pads ? (new TypeWidener())->nullable($expand->returns()) : $expand->returns();
            $references[$id] = new ResolvedReference($name, $type);
            $expandDefinitions[$name] = Definitions::of(
                $name,
                $type instanceof OptionalType ? $type->base() : $type,
                nullable: true,
            );
            $expands[$name] = $expand;
        }

        /** @var ScalarFunction $root an expand is rewritten into its ResolvedReference, any other node keeps its class */
        $root = self::rewrite($resolved, $references);

        return new self($root, $expands, $expandDefinitions);
    }

    /**
     * $gathered - the rows of output positions [$offset, $offset + count) - with the synthesized columns appended.
     *
     * @param array<string, list<mixed>> $cells from positions()
     */
    public function elements(Rows $gathered, array $cells, int $offset, Backend $backend): Rows
    {
        $columns = [];

        foreach ($cells as $name => $values) {
            $builder = $backend->builder($this->expandDefinitions[$name]);
            $builder->appendMany(array_slice($values, $offset, $gathered->count()));
            $columns[$name] = $builder->finish();
        }

        return $gathered->withColumns($gathered->schema()->add(...array_values($this->expandDefinitions)), $columns);
    }

    /**
     * The expand operands evaluated once: for every output position the row it comes from, and one cell per
     * synthesized column - zipped to the longest list, the shorter ones padded with null.
     *
     * @return array{list<int>, array<string, list<mixed>>} [$sources, $cells]
     */
    public function positions(Rows $rows, Backend $backend): array
    {
        $lists = [];

        foreach ($this->expands as $name => $expand) {
            $lists[$name] = (new Parameter($expand))->values($rows, $backend);
        }

        $sources = [];
        $cells = [];

        foreach ($lists as $name => $_) {
            $cells[$name] = [];
        }

        for ($index = 0, $count = $rows->count(); $index < $count; $index++) {
            $length = 0;

            foreach ($lists as $list) {
                $length = max($length, is_array($list[$index]) ? count($list[$index]) : 0);
            }

            foreach ($lists as $name => $list) {
                $elements = is_array($list[$index]) ? array_values($list[$index]) : [];

                for ($position = 0; $position < $length; $position++) {
                    $cells[$name][] = $elements[$position] ?? null;
                }
            }

            for ($position = 0; $position < $length; $position++) {
                $sources[] = $index;
            }
        }

        return [$sources, $cells];
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return $this->root->returns();
    }

    /**
     * The tree with every expand replaced by the synthesized column it reads its element from.
     */
    public function root(): ScalarFunction
    {
        return $this->root;
    }

    public function synthesized(): Schema
    {
        return new Schema(...array_values($this->expandDefinitions));
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
