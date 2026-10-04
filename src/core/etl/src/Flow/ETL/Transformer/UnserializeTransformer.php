<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\BoundStep;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Rows\RowsBuilder;
use Flow\ETL\Schema;
use Flow\ETL\Transformer;
use Flow\Serializer\Base64Serializer;
use Throwable;

use function Flow\ETL\DSL\ref;

final readonly class UnserializeTransformer implements Transformer
{
    /**
     * @param string $mergePrefix - used only when merge is set to true
     */
    public function __construct(
        private Reference|string $source,
        private Schema $target,
        private bool $merge = true,
        private string $mergePrefix = '',
    ) {}

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, (new UnpackedColumns())->of(
            $this->merge ? $input : new Schema(),
            $this->merge ? $this->mergePrefix : '',
            $this->target,
        ));
    }

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $context->telemetry()->transformationStarted($this);

        try {
            $source = $this->source instanceof Reference ? $this->source : ref($this->source);
            // base64 keeps serialized rows text-safe inside string entries, no matter which serializer is configured
            $serializer = new Base64Serializer($context->config->serializer());

            // a payload that fails to decode still has to produce the declared shape, so every declared
            // column is nullable - the same rule from_json follows
            $outputSchema = $this->bind($rows->schema())->output;
            $prefix = $this->merge ? $this->mergePrefix : '';
            $names = [];
            $outputNames = [];

            foreach ($this->target->definitions() as $definition) {
                $names[] = $definition->entry()->name();
                $outputNames[] = $prefix . $definition->entry()->name();
            }

            $decoder = SerializedPayloadDecoder::of($source, $serializer, $names, $prefix);
            $builder = new RowsBuilder($outputSchema->keep(...$outputNames), $context->backend());

            for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
                $builder->appendFrom($decoder->decode($rows, $i), 0);
            }

            $decoded = $builder->finish();
            $result = $this->merge ? $rows->withColumns($outputSchema, $decoded->columns()) : $decoded;

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
