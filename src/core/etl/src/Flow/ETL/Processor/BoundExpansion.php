<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ExpandingFunctions;
use Flow\ETL\Function\ReferencedColumns;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\ScalarFunction\UnpackResults;
use Flow\ETL\Plan\RequiredColumns;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\Expansion;
use Flow\ETL\Transformer\ScalarFunctionTransformer;
use Flow\ETL\Transformer\UnpackedColumns;
use Generator;

use function array_diff;
use function array_slice;
use function array_values;
use function count;
use function Flow\Types\DSL\type_instance_of;
use function in_array;

final readonly class BoundExpansion
{
    /**
     * @param list<string> $gathered input columns copied into every element row, in input order
     */
    public function __construct(
        public Expansion $expansion,
        public Transformer $inner,
        public Schema $output,
        public array $gathered,
    ) {}

    /**
     * @param Definition<mixed>|string $entry
     *
     * @throws InvalidLogicException when $function holds no expand
     */
    public static function of(
        string|Definition $entry,
        ScalarFunction $function,
        RequiredColumns $carries,
        Schema $input,
    ): self {
        $resolver = new ReferenceResolver();
        $resolved = $resolver->resolve($function, $input);
        $resolver->assertResolved($resolved, $input);
        (new ExpandingFunctions())->refuseNested($resolved);

        $expansion = Expansion::of($resolved, $input) ?? throw InvalidLogicException::because(
            '%s holds no array_expand(), it is not an expansion',
            $function::class,
        );
        $root = $expansion->root();
        $name = $entry instanceof Definition ? $entry->entry()->name() : $entry;
        $synthesized = $expansion->synthesized();
        $synthesizedNames = $synthesized->references()->names();
        $written = [$name];

        if ($root instanceof UnpackResults) {
            $written = [];

            foreach ((new UnpackedColumns())
                ->declared($root)
                ->references()
                ->names() as $unpacked) {
                $written[] = $name . '.' . $unpacked;
            }
        }

        $inner = new ScalarFunctionTransformer($entry, $root);
        $full = $inner->bind($input->add(...array_values($synthesized->definitions())))->output;
        $output = [];

        foreach ($full->definitions() as $definition) {
            $column = $definition->entry()->name();

            if (
                !in_array($column, $synthesizedNames, true)
                && (in_array($column, $written, true) || $carries->requires($column))
            ) {
                $output[] = $definition;
            }
        }

        $rootReads = array_diff((new ReferencedColumns())->in($root)->names(), $synthesizedNames);
        $gathered = [];

        foreach ($input->definitions() as $definition) {
            $column = $definition->entry()->name();

            if (
                !in_array($column, $written, true) && $carries->requires($column)
                || in_array($column, $rootReads, true)
            ) {
                $gathered[] = $column;
            }
        }

        return new self(
            $expansion,
            type_instance_of(Transformer::class)->assert($inner->bind($input->keep(...$gathered)->add(
                ...array_values($synthesized->definitions()),
            ))->step),
            new Schema(...$output),
            $gathered,
        );
    }

    /**
     * Output batches of at most $batchSize rows, built one at a time.
     *
     * @param int<1, max> $batchSize
     *
     * @return Generator<int, Rows>
     */
    public function chunks(Rows $rows, FlowContext $context, int $batchSize): Generator
    {
        [$sources, $cells] = $this->expansion->positions($rows, $context);

        if ($sources === []) {
            yield Rows::empty($this->output, $context->backend());

            return;
        }

        $present = [];

        // a batch may lack a nullable column of the bound schema; project() pads it
        foreach ($this->gathered as $column) {
            if ($rows->schema()->findDefinition($column) !== null) {
                $present[] = $column;
            }
        }

        $selected = $rows->select(...$present);

        for ($offset = 0, $total = count($sources); $offset < $total; $offset += $batchSize) {
            yield $this->inner->transform(
                $this->expansion->elements(
                    $selected->gather(array_slice($sources, $offset, $batchSize)),
                    $cells,
                    $offset,
                    $context->backend(),
                ),
                $context,
            )->project($this->output, $context->backend());
        }
    }
}
