<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\Serializer\Exception\SerializationException;
use Flow\Serializer\Serializer;

use function array_key_exists;
use function Flow\Serializer\DSL\unserialize_from_string;
use function is_string;

/**
 * A payload that does not decode still has to produce the declared shape, so every branch that gives
 * up answers with the declared columns as nulls - the rule from_json follows.
 */
final readonly class SerializedPayloadDecoder
{
    /**
     * @param array<string, string> $declared output column name => column name in the payload
     * @param array<string, null> $nulls
     */
    private function __construct(
        private Reference $source,
        private Serializer $serializer,
        private array $declared,
        private array $nulls,
    ) {}

    /**
     * @param array<string, string> $declared output column name => column name in the payload
     */
    public static function of(Reference $source, Serializer $serializer, array $declared): self
    {
        $nulls = [];

        foreach ($declared as $name => $_) {
            $nulls[$name] = null;
        }

        return new self($source, $serializer, $declared, $nulls);
    }

    /**
     * @return array<string, mixed>
     */
    public function decode(Row $row): array
    {
        if (!$row->has($this->source->name())) {
            return $this->nulls;
        }

        $serialized = $row->get($this->source->name());

        if (!is_string($serialized)) {
            return $this->nulls;
        }

        try {
            $decoded = unserialize_from_string($this->serializer, $serialized);
        } catch (SerializationException) {
            return $this->nulls;
        }

        if ($decoded->count() !== 1) {
            return $this->nulls;
        }

        $payload = $decoded->first()->values();
        $values = $this->nulls;

        foreach ($this->declared as $name => $payloadName) {
            if (array_key_exists($payloadName, $payload)) {
                $values[$name] = $payload[$payloadName];
            }
        }

        return $values;
    }
}
