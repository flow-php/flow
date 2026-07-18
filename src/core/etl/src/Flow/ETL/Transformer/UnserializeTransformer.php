<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Flow\Serializer\Base64Serializer;
use Flow\Serializer\Exception\SerializationException;
use Throwable;

use function Flow\ETL\DSL\ref;
use function Flow\Serializer\DSL\unserialize_from_string;
use function is_string;

final readonly class UnserializeTransformer implements Transformer
{
    /**
     * @param Reference|string $source
     * @param bool $merge
     * @param string $mergePrefix - used only when merge is set to true
     */
    public function __construct(
        private Reference|string $source,
        private bool $merge = true,
        private string $mergePrefix = '',
    ) {}

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $context->telemetry()->transformationStarted($this);

        try {
            $source = $this->source instanceof Reference ? $this->source : ref($this->source);
            // base64 keeps serialized rows text-safe inside string entries, no matter which serializer is configured
            $serializer = new Base64Serializer($context->config->serializer());

            $result = $rows->map(function (Row $row) use ($source, $serializer): Row {
                if (!$row->has($source->name())) {
                    return $row;
                }

                $serialized = $row->valueOf($source->name());

                if (!is_string($serialized)) {
                    return $row;
                }

                try {
                    $decoded = unserialize_from_string($serializer, $serialized);
                } catch (SerializationException) {
                    return $row;
                }

                // a payload that did not round-trip to a single Row is treated as a soft failure
                if ($decoded->count() !== 1) {
                    return $row;
                }

                return $this->merge ? $row->merge($decoded->first(), $this->mergePrefix) : $decoded->first();
            });

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
