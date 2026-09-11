<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\ColumnMismatchException;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\ScalarFunction\ExpandResults;
use Flow\ETL\Function\ScalarFunction\UnpackResults;
use Flow\ETL\Pipeline\BoundStep;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Transformer;
use Flow\Types\Type\Logical\StructureType;
use Throwable;

use function count;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\Types\DSL\type_array;
use function sprintf;

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
            $output = (new UnpackedColumns())->of($input, $this->entryName() . '.', $this->unpacked($resolved));

            return new BoundStep(new self($this->entry, $this->function, $resolved, null, $output), $output);
        }

        $resolved = $this->resolve($input);
        $derived = $this->derived($resolved);
        $output = $this->declare($input, $derived);

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
     * @param Definition<mixed> $derived
     */
    private function declare(Schema $input, Definition $derived): Schema
    {
        $name = $derived->entry()->name();

        return $input->findDefinition($name) === null ? $input->add($derived) : $input->replace($name, $derived);
    }

    /**
     * @return Definition<mixed>
     */
    private function derived(ScalarFunction $resolved): Definition
    {
        return $this->entry instanceof Definition
            ? $this->entry
            : definition_from_type($this->entryName(), $resolved->returns());
    }

    /**
     * @throws SchemaDefinitionNotFoundException
     */
    private function resolve(Schema $input): ScalarFunction
    {
        $resolver = new ReferenceResolver();
        $resolved = $resolver->resolve($this->function, $input);
        $resolver->assertResolved($resolved, $input);

        return $resolved;
    }

    private function entryName(): string
    {
        return $this->entry instanceof Definition ? $this->entry->entry()->name() : $this->entry;
    }

    /**
     * @param Definition<mixed> $derived
     *
     * @throws SchemaMismatchException
     */
    private function derivedValue(Definition $derived, mixed $value, int $rowIndex): mixed
    {
        // @mago-ignore analysis:mixed-assignment
        $cast = $value === null ? null : $derived->type()->cast($value);

        if (!$derived->matches($cast)) {
            throw new SchemaMismatchException($rowIndex, ColumnMismatchException::valueDoesNotMatch($derived, $cast));
        }

        return $cast;
    }

    private function map(Rows $rows, FlowContext $context): Rows
    {
        $function = $this->resolved ?? $this->resolve($rows->schema());
        $derived = $this->derived ?? $this->derived($function);
        $declared = $this->declare($rows->schema(), $derived);
        $output = $this->output ?? $declared;
        $name = $derived->entry()->name();
        $mapped = [];

        foreach ($rows->all() as $r) {
            if ($function instanceof ExpandResults) {
                // @mago-ignore analysis:mixed-assignment
                foreach (type_array()->assert($function->eval($r, $context)) as $val) {
                    $mapped[] = new Row([
                        ...$r->values(),
                        $name => $this->derivedValue($derived, $val, count($mapped)),
                    ]);
                }

                continue;
            }

            $mapped[] = new Row([
                ...$r->values(),
                $name => $this->derivedValue($derived, $function->eval($r, $context), count($mapped)),
            ]);
        }

        // Only the derived column is new, and derivedValue() checked it. Every other value passed the gate under
        // the same definition - unless the batch arrived under a schema other than the one bound, which the full
        // gate then conforms.
        return $declared->isSame($output) ? Rows::trusted($output, $mapped) : new Rows($output, ...$mapped);
    }

    /**
     * The columns unpack declares, read off returns() - the contract every ScalarFunction has.
     *
     * @throws InvalidLogicException
     */
    private function unpacked(ScalarFunction $resolved): Schema
    {
        $returns = $resolved->returns();

        if (!$returns instanceof StructureType) {
            throw new InvalidLogicException(sprintf(
                '%s unpacks into N columns, so returns() must be a StructureType, got "%s".',
                $resolved::class,
                $returns->toString(),
            ));
        }

        $definitions = [];

        foreach ($returns->elements() as $element) {
            $definitions[] = definition_from_type((string) $element->name, $element->type);
        }

        return new Schema(...$definitions);
    }

    private function unpack(Rows $rows, FlowContext $context): Rows
    {
        /** @var UnpackResults $function */
        $function = $this->resolved ?? $this->resolve($rows->schema());
        $declared = $this->unpacked($function)->definitions();
        $output = $this->output ?? (new UnpackedColumns())->of(
            $rows->schema(),
            $this->entryName() . '.',
            $this->unpacked($function),
        );
        $unpacked = [];

        foreach ($rows->all() as $r) {
            $values = $r->values();
            $payload = $function->eval($r, $context);

            foreach ($declared as $name => $definition) {
                // an undeclared payload key is dropped, a declared but absent one is null
                // @mago-ignore analysis:mixed-assignment
                $value = $payload[$name] ?? null;
                $values[$this->entryName() . '.' . $name] = $value === null ? null : $definition->type()->cast($value);
            }

            $unpacked[] = new Row($values);
        }

        return new Rows($output, ...$unpacked);
    }
}
