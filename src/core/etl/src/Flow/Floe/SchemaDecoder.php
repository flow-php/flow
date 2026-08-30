<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\Floe\Exception\FloeException;
use Flow\Types\Exception\InvalidTypeException;
use JsonException;

use function Flow\ETL\DSL\definition_from_array;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_list;
use function json_decode;

use const JSON_THROW_ON_ERROR;

final class SchemaDecoder
{
    public function __construct(
        private readonly ValueDecoder $valueDecoder,
    ) {}

    /**
     * @return array<int, ColumnBlueprint>
     */
    public function decode(string $schemaJson): array
    {
        try {
            // @mago-ignore analysis:mixed-assignment
            $definitions = json_decode($schemaJson, true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new FloeException('Floe failed to decode schema JSON: ' . $e->getMessage(), 0, $e);
        }

        try {
            $definitions = type_list(type_array())->assert($definitions);
        } catch (InvalidTypeException $e) {
            throw new FloeException(
                'Floe expected schema JSON to be a list of definitions: ' . $e->getMessage(),
                0,
                $e,
            );
        }

        $plan = [];

        /** @var array{ref: string, type: array<string, mixed>, nullable?: bool, metadata?: array<string, array<mixed>|bool|float|int|string>} $normalized */
        foreach ($definitions as $normalized) {
            $definition = definition_from_array([
                'ref' => $normalized['ref'],
                'type' => $normalized['type'],
                'nullable' => $normalized['nullable'] ?? false,
                'metadata' => $normalized['metadata'] ?? [],
            ]);

            $plan[] = new ColumnBlueprint(
                $normalized['ref'],
                $definition,
                $this->valueDecoder->decoderFor($definition),
            );
        }

        return $plan;
    }
}
