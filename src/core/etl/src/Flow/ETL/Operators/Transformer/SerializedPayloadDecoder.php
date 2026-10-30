<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Transformer;

use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Storage\Serializer\Exception\SerializationException;
use Flow\ETL\Storage\Serializer\Serializer;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Path;
use Flow\Filesystem\Stream\StringSourceStream;

use function base64_decode;
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
        private Rows $nothing,
    ) {}

    /**
     * @param list<string> $names the payload columns to read, each landing under $prefix . name
     */
    public static function of(Reference $source, Serializer $serializer, array $names, string $prefix = ''): self
    {
        return new self($source, $serializer, $names, $prefix, Rows::fromColumns(new Schema(), [], 1));
    }

    public function decode(Rows $rows, int $index): Rows
    {
        if ($rows->schema()->findDefinition($this->source->name()) === null) {
            return $this->nothing;
        }

        // @mago-ignore analysis:mixed-assignment
        $serialized = $rows->column($this->source->name())->value($index);

        if (!is_string($serialized)) {
            return $this->nothing;
        }

        $payload = base64_decode($serialized, true);

        if ($payload === false) {
            return $this->nothing;
        }

        try {
            $decoded = $this->serializer->unserialize(
                new StringSourceStream(Path::from('memory://serialized'), $payload),
            );
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

        return Rows::fromColumns(new Schema(...$definitions), $columns, 1);
    }
}
