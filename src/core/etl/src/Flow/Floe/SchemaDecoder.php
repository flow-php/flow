<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\DateEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\EnumEntry;
use Flow\ETL\Row\Entry\FloatEntry;
use Flow\ETL\Row\Entry\HTMLElementEntry;
use Flow\ETL\Row\Entry\HTMLEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\JsonEntry;
use Flow\ETL\Row\Entry\ListEntry;
use Flow\ETL\Row\Entry\MapEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Row\Entry\TimeEntry;
use Flow\ETL\Row\Entry\UuidEntry;
use Flow\ETL\Row\Entry\XMLElementEntry;
use Flow\ETL\Row\Entry\XMLEntry;
use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Definition\DateDefinition;
use Flow\ETL\Schema\Definition\DateTimeDefinition;
use Flow\ETL\Schema\Definition\EnumDefinition;
use Flow\ETL\Schema\Definition\FloatDefinition;
use Flow\ETL\Schema\Definition\HTMLDefinition;
use Flow\ETL\Schema\Definition\HTMLElementDefinition;
use Flow\ETL\Schema\Definition\IntegerDefinition;
use Flow\ETL\Schema\Definition\JsonDefinition;
use Flow\ETL\Schema\Definition\ListDefinition;
use Flow\ETL\Schema\Definition\MapDefinition;
use Flow\ETL\Schema\Definition\StringDefinition;
use Flow\ETL\Schema\Definition\StructureDefinition;
use Flow\ETL\Schema\Definition\TimeDefinition;
use Flow\ETL\Schema\Definition\UuidDefinition;
use Flow\ETL\Schema\Definition\XMLDefinition;
use Flow\ETL\Schema\Definition\XMLElementDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Floe\Exception\FloeException;
use Flow\Types\Exception\InvalidTypeException;
use JsonException;

use function Flow\ETL\DSL\definition_from_array;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_list;
use function json_decode;
use function sprintf;

use const JSON_THROW_ON_ERROR;

final class SchemaDecoder
{
    private const array ENTRY_CLASSES = [
        BooleanDefinition::class => BooleanEntry::class,
        DateDefinition::class => DateEntry::class,
        DateTimeDefinition::class => DateTimeEntry::class,
        EnumDefinition::class => EnumEntry::class,
        FloatDefinition::class => FloatEntry::class,
        HTMLDefinition::class => HTMLEntry::class,
        HTMLElementDefinition::class => HTMLElementEntry::class,
        IntegerDefinition::class => IntegerEntry::class,
        JsonDefinition::class => JsonEntry::class,
        ListDefinition::class => ListEntry::class,
        MapDefinition::class => MapEntry::class,
        StringDefinition::class => StringEntry::class,
        StructureDefinition::class => StructureEntry::class,
        TimeDefinition::class => TimeEntry::class,
        UuidDefinition::class => UuidEntry::class,
        XMLDefinition::class => XMLEntry::class,
        XMLElementDefinition::class => XMLElementEntry::class,
    ];

    public function __construct(
        private readonly ValueDecoder $valueDecoder,
        private readonly EntryInstantiator $entryInstantiator,
    ) {}

    /**
     * @return array<int, HydratorColumn>
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
            $metadata = $normalized['metadata'] ?? [];
            unset($metadata[Metadata::FROM_NULL]);

            $definition = definition_from_array([
                'ref' => $normalized['ref'],
                'type' => $normalized['type'],
                'nullable' => false,
                'metadata' => $metadata,
            ]);

            $entryClass = self::ENTRY_CLASSES[$definition::class] ?? throw new FloeException(sprintf(
                'Floe cannot hydrate entries for definition "%s"',
                $definition::class,
            ));

            $fromNullDefinition = $definition->makeNullable();
            $fromNullDefinition->setMetadata(
                $fromNullDefinition->metadata()->merge(Metadata::fromArray([Metadata::FROM_NULL => true])),
            );

            /** @var class-string<Entry<mixed>> $entryClass */
            $plan[] = new HydratorColumn(
                $normalized['ref'],
                $definition,
                $definition->makeNullable(),
                $fromNullDefinition,
                $this->valueDecoder->decoderFor($definition->type()),
                $this->entryInstantiator->factoryFor($entryClass),
            );
        }

        return $plan;
    }
}
