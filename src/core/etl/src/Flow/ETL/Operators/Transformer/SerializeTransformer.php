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
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition\StringDefinition;
use Flow\Filesystem\Path;
use Flow\Filesystem\Stream\StringDestinationStream;
use SensitiveParameter;

use function base64_encode;
use function Flow\Types\DSL\type_string;
use function is_array;

final readonly class SerializeTransformer implements Transformer
{
    public function __construct(
        private Reference|string $target,
        private Serializer $serializer,
    ) {}

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep(
            $this,
            $input->put(
                new StringDefinition($this->target instanceof Reference ? $this->target->name() : $this->target),
            ),
        );
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        // @mago-ignore analysis:mixed-assignment
        $target = $data['target'] ?? null;

        return new self(
            is_array($target) ? $rebuild->of($target, Reference::class) : type_string()->assert($target),
            $rebuild->of($data['serializer'] ?? null, Serializer::class),
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
                    'target' => $this->target,
                    'serializer' => $this->serializer,
                ])
                : null
        );
    }

    public function transform(Rows $rows, Backend $backend): Rows
    {
        $target = $this->target instanceof Reference ? $this->target : new UnresolvedReference($this->target);
        $column = new StringDefinition($target->name());
        $outputSchema = $this->bind($rows->schema())->output;

        $serialized = $backend->builder($column);

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            // base64 keeps serialized rows text-safe inside string columns, no matter which serializer is configured
            $destination = new StringDestinationStream(Path::from('memory://serialized'));
            $this->serializer->serialize($rows->slice($i, 1), $destination);
            $serialized->append(base64_encode($destination->content()));
        }

        return $rows->withColumns($outputSchema, [$target->name() => $serialized->finish()]);
    }
}
