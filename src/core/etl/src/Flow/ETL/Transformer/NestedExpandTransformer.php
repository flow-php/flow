<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\BoundStep;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Rows\RowsBuilder;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Transformer;
use Throwable;

use function count;
use function Flow\Types\DSL\type_array;

final readonly class NestedExpandTransformer implements Transformer
{
    /**
     * @param Definition<mixed>|Schema $declares
     */
    public function __construct(
        private ScalarFunctionTransformer $unbound,
        private string $entryName,
        private NestedExpansion $expansion,
        private Definition|Schema $declares,
        private Schema $output,
    ) {}

    public function bind(Schema $input): BoundStep
    {
        return $this->unbound->bind($input);
    }

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $context->telemetry()->transformationStarted($this, [
            TelemetryAttributes::ATTR_SCALAR_FUNCTION => $this->unbound->function::class,
        ]);

        try {
            $result = $this->declares instanceof Definition
                ? $this->map($rows, $context, $this->declares)
                : $this->unpack($rows, $context, $this->declares);

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
    private function map(Rows $rows, FlowContext $context, Definition $derived): Rows
    {
        $columns = new DerivedColumns();
        $declared = $columns->declare($rows->schema(), $derived);
        $cast = [];
        $sources = [];

        foreach ($rows->all() as $index => $r) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($this->expansion->eval($r, $context) as $value) {
                $sources[] = $index;
                $cast[] = $columns->value($derived, $value, count($cast));
            }
        }

        $builder = $context->backend()->builder($derived);
        $builder->appendMany($cast);

        return $columns->rows(
            $rows->gather($sources),
            $declared,
            $this->output,
            $derived->entry()->name(),
            $builder->finish(),
        );
    }

    private function unpack(Rows $rows, FlowContext $context, Schema $declared): Rows
    {
        $columns = new UnpackedColumns();
        $prefix = $this->entryName . '.';
        $prefixed = [];

        foreach ($declared->definitions() as $name => $_) {
            $prefixed[] = $prefix . $name;
        }

        $payloads = [];
        $sources = [];

        foreach ($rows->all() as $index => $r) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($this->expansion->eval($r, $context) as $payload) {
                $sources[] = $index;
                $payloads[] = $columns->values($prefix, $declared, type_array()->assert($payload));
            }
        }

        return $rows->gather($sources)->withColumns(
            $this->output,
            (new RowsBuilder($this->output->keep(...$prefixed), $context->backend()))
                ->appendRows($payloads)
                ->finish()
                ->columns(),
        );
    }
}
