<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\BoundStep;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ExpandingFunctions;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\ScalarFunction\UnpackResults;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Transformer;
use Throwable;

use function Flow\ETL\DSL\definition_from_type;
use function Flow\Types\DSL\type_array;

final readonly class ScalarFunctionTransformer implements Transformer
{
    /**
     * @param Definition<mixed>|string $entry
     * @param null|ScalarFunction $resolved the function resolved against the bound schema
     * @param null|Definition<mixed> $derived the column this step declares
     * @param null|Schema $output the schema this step declares
     */
    public function __construct(
        private string|Definition $entry,
        public ScalarFunction $function,
        private ?ScalarFunction $resolved = null,
        private ?Definition $derived = null,
        private ?Schema $output = null,
    ) {}

    public function bind(Schema $input): BoundStep
    {
        // unpack declares its own columns, so the check comes first - a Definition passed to
        // withEntry() cannot describe an N-column result
        if ($this->function instanceof UnpackResults) {
            $resolved = $this->resolve($input);
            $output = (new UnpackedColumns())->of(
                $input,
                $this->entryName() . '.',
                (new UnpackedColumns())->declared($resolved),
            );

            return new BoundStep(new self($this->entry, $this->function, $resolved, null, $output), $output);
        }

        $resolved = $this->resolve($input);
        $derived = $this->derived($resolved);
        $output = (new DerivedColumns())->declare($input, $derived);

        return new BoundStep(new self($this->entry, $this->function, $resolved, $derived, $output), $output);
    }

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $context->telemetry()->transformationStarted($this, [
            TelemetryAttributes::ATTR_SCALAR_FUNCTION => $this->function::class,
        ]);

        try {
            $result = $this->function instanceof UnpackResults
                ? $this->unpack($rows, $context)
                : $this->map($rows, $context);

            $context->telemetry()->transformationCompleted($this, [
                TelemetryAttributes::ATTR_TRANSFORMATION_INPUT_ROWS => $rows->count(),
                TelemetryAttributes::ATTR_TRANSFORMATION_OUTPUT_ROWS => $result->count(),
            ]);

            return $result;
        } catch (Throwable $e) {
            $context->telemetry()->transformationFailed($this, $e);

            throw $e;
        }
    }

    /**
     * @return Definition<mixed>
     */
    private function derived(ScalarFunction $source): Definition
    {
        return $this->entry instanceof Definition
            ? $this->entry
            : definition_from_type($this->entryName(), $source->returns());
    }

    /**
     * @throws SchemaDefinitionNotFoundException
     * @throws InvalidArgumentException
     */
    private function resolve(Schema $input): ScalarFunction
    {
        $resolver = new ReferenceResolver();
        $resolved = $resolver->resolve($this->function, $input);
        $resolver->assertResolved($resolved, $input);
        (new ExpandingFunctions())->refuse($resolved, 'transform');

        return $resolved;
    }

    private function entryName(): string
    {
        return $this->entry instanceof Definition ? $this->entry->entry()->name() : $this->entry;
    }

    private function map(Rows $rows, FlowContext $context): Rows
    {
        $columns = new DerivedColumns();
        $function = $this->resolved ?? $this->resolve($rows->schema());
        $derived = $this->derived ?? $this->derived($function);
        $declared = $columns->declare($rows->schema(), $derived);
        $output = $this->output ?? $declared;
        return $columns->rows(
            $rows,
            $declared,
            $output,
            $derived->entry()->name(),
            $columns->stored($derived, $function->eval($rows, $context), $context->backend()),
            $context->backend(),
        );
    }

    private function unpack(Rows $rows, FlowContext $context): Rows
    {
        /** @var UnpackResults $function */
        $function = $this->resolved ?? $this->resolve($rows->schema());
        $declared = (new UnpackedColumns())->declared($function);
        $columns = new UnpackedColumns();
        $output = $this->output ?? $columns->of($rows->schema(), $this->entryName() . '.', $declared);
        $prefix = $this->entryName() . '.';
        $values = [];

        foreach ($declared->definitions() as $name => $_) {
            $values[$prefix . $name] = [];
        }

        // @mago-ignore analysis:mixed-assignment
        foreach ($function->eval($rows, $context)->values() as $payload) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($columns->values($prefix, $declared, type_array()->assert($payload)) as $name => $value) {
                $values[$name][] = $value;
            }
        }

        $unpacked = [];

        foreach ($values as $name => $columnValues) {
            $builder = $context->backend()->builder($output->get((string) $name));
            $builder->appendMany($columnValues);
            $unpacked[$name] = $builder->finish();
        }

        return $rows->withColumns($output, $unpacked);
    }
}
