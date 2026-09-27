<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Serializer\Exception\SerializationException;
use Flow\Serializer\Serializer;

use function Flow\Serializer\DSL\unserialize_from_string;
use function is_string;

final readonly class SerializedPayloadDecoder
{
    /**
     * @param list<string> $names the payload columns to read
     */
    private function __construct(
        private Reference $source,
        private Serializer $serializer,
        private array $names,
        private string $prefix,
        private Row $nothing,
    ) {}

    /**
     * @param list<string> $names the payload columns to read, each landing under $prefix . name
     */
    public static function of(Reference $source, Serializer $serializer, array $names, string $prefix = ''): self
    {
        return new self($source, $serializer, $names, $prefix, Rows::fromColumns(new Schema(), [], 1)->row(0));
    }

    /**
     * A view of the payload's declared columns under their output names; a payload that cannot be read is a view with
     * no columns at all.
     */
    public function decode(Row $row): Row
    {
        if (!$row->has($this->source->name())) {
            return $this->nothing;
        }

        $serialized = $row->get($this->source->name());

        if (!is_string($serialized)) {
            return $this->nothing;
        }

        try {
            $decoded = unserialize_from_string($this->serializer, $serialized);
        } catch (SerializationException) {
            return $this->nothing;
        }

        if ($decoded->count() !== 1) {
            return $this->nothing;
        }

        $definitions = [];
        $columns = [];

        foreach ($this->names as $name) {
            $definition = $decoded->schema()->findDefinition($name);

            if ($definition === null) {
                continue;
            }

            $definitions[] = $definition->rename($this->prefix . $name);
            $columns[$this->prefix . $name] = $decoded->column($name);
        }

        return Rows::fromColumns(new Schema(...$definitions), $columns, 1)->row(0);
    }
}
