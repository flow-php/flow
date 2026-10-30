<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Processor;

use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\Functions\ExpandingFunctions;
use Flow\ETL\Functions\ReferencedColumns;
use Flow\ETL\Functions\ReferenceResolver;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Functions\ScalarFunction\UnpackResults;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Operators\Transformer\Expansion;
use Flow\ETL\Operators\Transformer\ScalarFunctionTransformer;
use Flow\ETL\Operators\Transformer\UnpackedColumns;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition;
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
     * @param Schema $input the schema the expansion was bound to
     */
    public function __construct(
        public Expansion $expansion,
        public Transformer $inner,
        public Schema $output,
        public array $gathered,
        public Schema $input,
    ) {}

    /**
     * @param Definition<mixed>|string $column
     *
     * @throws InvalidLogicException when $function holds no expand
     */
    public static function of(
        string|Definition $column,
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
        $name = $column instanceof Definition ? $column->name() : $column;
        $synthesized = $expansion->synthesized();
        $synthesizedNames = $synthesized->names();
        $written = [$name];

        if ($root instanceof UnpackResults) {
            $written = [];

            foreach ((new UnpackedColumns())
                ->declared($root)
                ->names() as $unpacked) {
                $written[] = $name . '.' . $unpacked;
            }
        }

        $inner = new ScalarFunctionTransformer($column, $root);
        $full = $inner->bind($input->add(...array_values($synthesized->definitions())))->output;
        $output = [];

        foreach ($full->definitions() as $definition) {
            $columnName = $definition->name();

            if (
                !in_array($columnName, $synthesizedNames, true)
                && (in_array($columnName, $written, true) || $carries->requires($columnName))
            ) {
                $output[] = $definition;
            }
        }

        $rootReads = array_diff((new ReferencedColumns())->in($root)->names(), $synthesizedNames);
        $gathered = [];

        foreach ($input->definitions() as $definition) {
            $columnName = $definition->name();

            if (
                !in_array($columnName, $written, true) && $carries->requires($columnName)
                || in_array($columnName, $rootReads, true)
            ) {
                $gathered[] = $columnName;
            }
        }

        return new self(
            $expansion,
            type_instance_of(Transformer::class)->assert($inner->bind($input->keep(...$gathered)->add(
                ...array_values($synthesized->definitions()),
            ))->step),
            new Schema(...$output),
            $gathered,
            $input,
        );
    }

    /**
     * Output batches of at most $batchSize rows, built one at a time.
     *
     * @param int<1, max> $batchSize
     *
     * @return Generator<int, Rows>
     */
    public function chunks(Rows $rows, Backend $backend, int $batchSize): Generator
    {
        [$sources, $cells] = $this->expansion->positions($rows, $backend);

        if ($sources === []) {
            yield Rows::empty($this->output, $backend);

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
                    $backend,
                ),
                $backend,
            )->project($this->output, $backend);
        }
    }
}
