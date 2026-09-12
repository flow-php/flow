<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ExpandingFunctions;
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
            $unpacked = $this->unpacked($resolved);
            $output = (new UnpackedColumns())->of($input, $this->entryName() . '.', $unpacked);
            $expansion = NestedExpansion::of($resolved, $input);

            return new BoundStep(
                $expansion === null
                    ? new self($this->entry, $this->function, $resolved, null, $output)
                    : new NestedExpandTransformer($this, $this->entryName(), $expansion, $unpacked, $output),
                $output,
            );
        }

        $resolved = $this->resolve($input);
        $expansion = NestedExpansion::of($resolved, $input);
        $derived = $this->derived($expansion ?? $resolved);
        $output = (new DerivedColumns())->declare($input, $derived);

        return new BoundStep(
            $expansion === null
                ? new self($this->entry, $this->function, $resolved, $derived, $output)
                : new NestedExpandTransformer($this, $this->entryName(), $expansion, $derived, $output),
            $output,
        );
    }

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        // only bind() can tell a nested expand from a root one
        if ($this->resolved === null && (new ExpandingFunctions())->in($this->function) !== []) {
            /** @var Transformer $bound bind() plans a ScalarFunctionTransformer or a NestedExpandTransformer */
            $bound = $this->bind($rows->schema())->step;

            return $bound->transform($rows, $context);
        }

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
    private function derived(ScalarFunction|NestedExpansion $source): Definition
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
        (new ExpandingFunctions())->refuseNested($resolved);

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
        $name = $derived->entry()->name();
        $mapped = [];

        foreach ($rows->all() as $r) {
            if ($function instanceof ExpandResults) {
                // @mago-ignore analysis:mixed-assignment
                foreach (type_array()->assert($function->eval($r, $context)) as $val) {
                    $mapped[] = new Row([
                        ...$r->values(),
                        $name => $columns->value($derived, $val, count($mapped)),
                    ]);
                }

                continue;
            }

            $mapped[] = new Row([
                ...$r->values(),
                $name => $columns->value($derived, $function->eval($r, $context), count($mapped)),
            ]);
        }

        return $columns->rows($declared, $output, $mapped);
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
        $declared = $this->unpacked($function);
        $columns = new UnpackedColumns();
        $output = $this->output ?? $columns->of($rows->schema(), $this->entryName() . '.', $declared);
        $unpacked = [];

        foreach ($rows->all() as $r) {
            $unpacked[] = new Row($columns->values(
                $r->values(),
                $this->entryName() . '.',
                $declared,
                $function->eval($r, $context),
            ));
        }

        return new Rows($output, ...$unpacked);
    }
}
