<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Schema;
use Flow\Floe\Exception\FloeException;
use Flow\Types\Type;
use JsonException;

use function array_values;
use function json_encode;

use const JSON_THROW_ON_ERROR;

final class SchemaEncoder
{
    public function __construct(
        private readonly ValueEncoder $valueEncoder,
    ) {}

    /**
     * @throws FloeException
     */
    public function encodeSchema(Schema $schema): EncoderPlan
    {
        try {
            $columns = [];
            $definitions = [];

            foreach (array_values($schema->definitions()) as $definition) {
                $name = $definition->entry()->name();
                $columns[$name] = $this->column($name, $definition->type());
                $definitions[] = $definition->normalize();
            }

            return new EncoderPlan($columns, json_encode($definitions, JSON_THROW_ON_ERROR));
        } catch (JsonException $e) {
            throw new FloeException('Floe failed to encode schema as JSON: ' . $e->getMessage(), 0, $e);
        }
    }

    /**
     * @param Type<mixed> $type
     *
     * @throws JsonException
     */
    private function column(string $name, Type $type): EncoderColumn
    {
        return new EncoderColumn(
            $name,
            json_encode($type->normalize(), JSON_THROW_ON_ERROR),
            $this->valueEncoder->encoderFor($type),
        );
    }
}
