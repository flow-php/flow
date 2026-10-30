<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Transformer;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Foundation\Telemetry\SpanAttributes;
use Flow\ETL\Foundation\Telemetry\TelemetryAttributes;
use Flow\ETL\Functions\ReferenceResolver;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Functions\ScalarFunction\UnpackResults;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Definitions;
use SensitiveParameter;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_string;
use function is_array;

final readonly class ScalarFunctionTransformer implements Transformer, SpanAttributes
{
    /**
     * @param Definition<mixed>|string $column
     * @param null|ScalarFunction $resolved the function resolved against the bound schema
     * @param null|Definition<mixed> $derived the column this step declares
     * @param null|Schema $output the schema this step declares
     * @param null|Schema $declared the columns an unpack declares, before the prefix
     */
    public function __construct(
        private string|Definition $column,
        public ScalarFunction $function,
        private ?ScalarFunction $resolved = null,
        private ?Definition $derived = null,
        private ?Schema $output = null,
        private ?Schema $declared = null,
    ) {}

    public function bind(Schema $input): BoundStep
    {
        // unpack declares its own columns, so the check comes first - a Definition passed to
        // withColumn() cannot describe an N-column result
        if ($this->function instanceof UnpackResults) {
            $resolved = $this->resolve($input);
            $columns = new UnpackedColumns();
            $declared = $columns->declared($resolved);
            $output = $columns->of($input, $this->columnName() . '.', $declared);

            return new BoundStep(
                new self($this->column, $this->function, $resolved, null, $output, $declared),
                $output,
            );
        }

        $resolved = $this->resolve($input);
        $derived = $this->derived($resolved);
        $output = $input->put($derived);

        return new BoundStep(new self($this->column, $this->function, $resolved, $derived, $output), $output);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        // @mago-ignore analysis:mixed-assignment
        $column = $data['column'] ?? null;

        return new self(
            is_array($column) ? Definitions::fromArray($column) : type_string()->assert($column),
            $rebuild->of($data['function'] ?? null, ScalarFunction::class),
            ($data['resolved'] ?? null) === null ? null : $rebuild->of($data['resolved'], ScalarFunction::class),
            ($data['derived'] ?? null) === null ? null : Definitions::fromArray(type_array()->assert($data['derived'])),
            ($data['output'] ?? null) === null ? null : Schema::fromArray(type_array()->assert($data['output'])),
            ($data['declared'] ?? null) === null ? null : Schema::fromArray(type_array()->assert($data['declared'])),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'column' => $this->column instanceof Definition ? $this->column->normalize() : $this->column,
            'function' => $this->function,
            'resolved' => $this->resolved,
            'derived' => $this->derived?->normalize(),
            'output' => $this->output?->normalize(),
            'declared' => $this->declared?->normalize(),
        ]);
    }

    public function spanAttributes(): array
    {
        return [TelemetryAttributes::ATTR_SCALAR_FUNCTION => $this->function::class];
    }

    public function transform(Rows $rows, Backend $backend): Rows
    {
        return $this->function instanceof UnpackResults ? $this->unpack($rows, $backend) : $this->map($rows, $backend);
    }

    /**
     * @return Definition<mixed>
     */
    private function derived(ScalarFunction $source): Definition
    {
        return $this->column instanceof Definition
            ? $this->column
            : Definitions::of($this->columnName(), $source->returns());
    }

    /**
     * @throws SchemaDefinitionNotFoundException
     * @throws InvalidArgumentException
     */
    private function resolve(Schema $input): ScalarFunction
    {
        return (new ReferenceResolver())->bind($this->function, $input, 'transform');
    }

    private function columnName(): string
    {
        return $this->column instanceof Definition ? $this->column->name() : $this->column;
    }

    private function map(Rows $rows, Backend $backend): Rows
    {
        $columns = new DerivedColumns();
        $function = $this->resolved ?? $this->resolve($rows->schema());
        $derived = $this->derived ?? $this->derived($function);
        $declared = $rows->schema()->put($derived);
        return $columns->rows(
            $rows,
            $declared,
            $this->output ?? $declared,
            $derived->name(),
            $columns->stored($derived, $function->eval($rows, $backend), $backend),
            $backend,
        );
    }

    private function unpack(Rows $rows, Backend $backend): Rows
    {
        /** @var UnpackResults $function */
        $function = $this->resolved ?? $this->resolve($rows->schema());
        $columns = new UnpackedColumns();
        $declared = $this->declared ?? $columns->declared($function);
        $output = $this->output ?? $columns->of($rows->schema(), $this->columnName() . '.', $declared);
        $prefix = $this->columnName() . '.';
        $values = [];

        foreach ($declared->definitions() as $name => $_) {
            $values[$prefix . $name] = [];
        }

        // @mago-ignore analysis:mixed-assignment
        foreach ($function->eval($rows, $backend)->values() as $payload) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($columns->values($prefix, $declared, type_array()->assert($payload)) as $name => $value) {
                $values[$name][] = $value;
            }
        }

        $unpacked = [];

        foreach ($values as $name => $columnValues) {
            $builder = $backend->builder($output->get((string) $name));
            $builder->appendMany($columnValues);
            $unpacked[$name] = $builder->finish();
        }

        return $rows->withColumns($output, $unpacked);
    }
}
