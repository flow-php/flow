<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\BoundStep;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Transformer;
use Flow\Serializer\Base64Serializer;
use Throwable;

use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Serializer\DSL\serialize_to_string;

final readonly class SerializeTransformer implements Transformer
{
    public function __construct(
        private Reference|string $target,
        private bool $standalone = false,
    ) {}

    public function bind(Schema $input): BoundStep
    {
        $name = $this->target instanceof Reference ? $this->target->name() : $this->target;
        $column = str_schema($name);

        if ($this->standalone) {
            return new BoundStep($this, schema($column));
        }

        return new BoundStep(
            $this,
            $input->findDefinition($name) === null ? $input->add($column) : $input->replace($name, $column),
        );
    }

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $context->telemetry()->transformationStarted($this);

        try {
            $target = $this->target instanceof Reference ? $this->target : ref($this->target);
            // base64 keeps serialized rows text-safe inside string entries, no matter which serializer is configured
            $serializer = new Base64Serializer($context->config->serializer());

            $inputSchema = $rows->schema();
            $column = str_schema($target->name());
            $outputSchema = $this->standalone
                ? schema($column)
                : (
                    $inputSchema->findDefinition($target->name()) === null
                        ? $inputSchema->add($column)
                        : $inputSchema->replace($target->name(), $column)
                );

            $serialized = [];

            foreach ($rows->all() as $row) {
                $payload = serialize_to_string($serializer, rows($inputSchema, $row));

                $serialized[] = $this->standalone
                    ? row([$target->name() => $payload])
                    : row([...$row->values(), $target->name() => $payload]);
            }

            $result = new Rows($outputSchema, ...$serialized);

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
}
