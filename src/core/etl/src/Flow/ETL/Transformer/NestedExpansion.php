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
use Flow\ETL\Schema\Definition;
use Flow\Types\Type;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\TypeWidener;

use function array_fill;
use function array_key_exists;
use function array_map;
use function array_values;
use function count;
use function Flow\ETL\DSL\definition_from_type;
use function max;
use function spl_object_id;

final readonly class NestedExpansion
{
    /**
     * @param non-empty-array<string, ExpandResults> $expands keyed by the synthesized column the root reads
     *                                                        each element from
     * @param array<string, Definition<mixed>> $expandDefinitions the synthesized columns, keyed like $expands
     * @param Schema $elementSchema $input with the synthesized columns, built once
     */
    private function __construct(
        private ScalarFunction $root,
        private array $expands,
        private Schema $input,
        private array $expandDefinitions,
        private Schema $elementSchema,
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
        $expandDefinitions = [];
        $suffix = 0;

        foreach ($distinct as $id => $expand) {
            do {
                $name = "\0expand:" . $suffix++;
            } while ($input->findDefinition($name) !== null);

            $type = $pads ? (new TypeWidener())->nullable($expand->returns()) : $expand->returns();
            $references[$id] = new ResolvedReference($name, $type);
            $expandDefinitions[$name] = definition_from_type(
                $name,
                $type instanceof OptionalType ? $type->base() : $type,
                nullable: true,
            );
            $expands[$name] = $expand;
        }

        /** @var ScalarFunction $root the root is not an expand, so rewrite() rebuilds it with its own class */
        $root = self::rewrite($resolved, $references);

        return new self($root, $expands, $input, $expandDefinitions, $input->add(...array_values($expandDefinitions)));
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

        $length = max(array_map(count(...), $lists));
        $elements = $row->rows->gather(array_fill(0, $length, $row->index));
        $schema = $elements->schema()->isSame($this->input)
            ? $this->elementSchema
            : $elements->schema()->add(...array_values($this->expandDefinitions));
        $columns = [];

        foreach ($lists as $name => $list) {
            $builder = $context->backend()->builder($this->expandDefinitions[$name]);
            $cells = [];

            for ($position = 0; $position < $length; $position++) {
                $cells[] = $list[$position] ?? null;
            }

            $builder->appendMany($cells);
            $columns[$name] = $builder->finish();
        }

        $batch = $elements->withColumns($schema, $columns);
        $values = [];

        for ($position = 0; $position < $length; $position++) {
            $values[] = $this->root->eval($batch->row($position), $context);
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
