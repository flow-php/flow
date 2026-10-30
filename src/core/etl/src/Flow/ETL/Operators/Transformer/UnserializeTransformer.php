<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Transformer;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Reference\UnresolvedReference;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Storage\Serializer\Serializer;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Rows\RowsBuilder;
use Flow\ETL\Values\Schema;
use SensitiveParameter;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_string;
use function is_array;

final readonly class UnserializeTransformer implements Transformer
{
    public function __construct(
        private Reference|string $source,
        private Schema $target,
        private Serializer $serializer,
        private string $mergePrefix = '',
    ) {}

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, (new UnpackedColumns())->of($input, $this->mergePrefix, $this->target));
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        // @mago-ignore analysis:mixed-assignment
        $source = $data['source'] ?? null;

        return new self(
            is_array($source) ? $rebuild->of($source, Reference::class) : type_string()->assert($source),
            Schema::fromArray(type_array()->assert($data['target'] ?? null)),
            $rebuild->of($data['serializer'] ?? null, Serializer::class),
            type_string()->assert($data['merge_prefix'] ?? null),
        );
    }

    /**
     * null when the serializer does not describe.
     */
    public function describe(): ?Description
    {
        return (
            $this->serializer instanceof Describable
                ? Description::compose(self::class, [
                    'source' => $this->source,
                    'target' => $this->target->normalize(),
                    'serializer' => $this->serializer,
                    'merge_prefix' => $this->mergePrefix,
                ])
                : null
        );
    }

    public function transform(Rows $rows, Backend $backend): Rows
    {
        $source = $this->source instanceof Reference ? $this->source : new UnresolvedReference($this->source);
        // a payload that fails to decode still has to produce the declared shape, so every declared
        // column is nullable - the same rule from_json follows
        $outputSchema = $this->bind($rows->schema())->output;
        $names = [];
        $outputNames = [];

        foreach ($this->target->definitions() as $definition) {
            $names[] = $definition->name();
            $outputNames[] = $this->mergePrefix . $definition->name();
        }

        $decoder = SerializedPayloadDecoder::of($source, $this->serializer, $names, $this->mergePrefix);
        $builder = new RowsBuilder($outputSchema->keep(...$outputNames), $backend);

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            $builder->appendFrom($decoder->decode($rows, $i), 0);
        }

        return $rows->withColumns($outputSchema, $builder->finish()->columns());
    }
}
