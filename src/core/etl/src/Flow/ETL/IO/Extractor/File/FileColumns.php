<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Definition\StringDefinition;
use Flow\ETL\Values\Schema\Definitions;
use Flow\Filesystem\Path;
use Throwable;

use function array_key_exists;
use function array_keys;
use function array_map;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function sprintf;

/**
 * Both read paths take the tail from one instance of this, in one order, so they cannot disagree.
 */
final readonly class FileColumns
{
    /**
     * @param array<string, bool> $partitionNames
     */
    public function __construct(
        private array $partitionNames,
        private PartitionTypes $partitionTypes,
        private bool $metadataColumns,
    ) {}

    /**
     * @param array<array-key, mixed> $data the shape of normalize()
     */
    public static function fromArray(array $data): self
    {
        return new self(
            type_map(type_string(), type_boolean())->assert($data['partition_names'] ?? null),
            PartitionTypes::fromArray(type_array()->assert($data['partition_types'] ?? null)),
            type_boolean()->assert($data['metadata_columns'] ?? null),
        );
    }

    public function declare(Schema $schema): Schema
    {
        $schema = $this->metadataColumns ? $schema->add(new StringDefinition('_input_file_uri')) : $schema;
        $this->partitionTypes->assertEveryNameIsAPartition($this->partitionNames);

        if ($this->partitionNames === []) {
            return $schema;
        }

        $definitions = [];

        foreach ($this->partitionNames as $name => $nullable) {
            // 1) a declared schema wins, 2) then a declared partition type, 3) then string
            $definitions[] =
                $schema->findDefinition($name)
                ?? (
                    $this->partitionTypes->has($name)
                        ? Definitions::of($name, $this->partitionTypes->get($name), nullable: $nullable)
                        : new StringDefinition($name, nullable: $nullable)
                );
        }

        return $schema->gracefulRemove(...array_keys($this->partitionNames))->add(...$definitions);
    }

    /**
     * $value as $definition types it: kept when it matches, cast otherwise; a missing or null value is NULL.
     *
     * @throws InvalidArgumentException when $value cannot be cast to $definition's type
     */
    public static function partitionValue(Definition $definition, string $name, ?string $value, Path $file): mixed
    {
        if ($definition->matches($value)) {
            return $value;
        }

        try {
            return $definition->type()->cast($value);
        } catch (Throwable $e) {
            throw new InvalidArgumentException(
                sprintf(
                    'Partition column "%s" of file "%s" declares type %s, but its path value %s cannot be cast to it.',
                    $name,
                    $file->uri(),
                    $definition->type()->toString(),
                    $value === null ? 'NULL' : '"' . $value . '"',
                ),
                0,
                $e,
            );
        }
    }

    /**
     * @return array{partition_names: array<string, bool>, partition_types: array<string, array<string, mixed>>, metadata_columns: bool}
     */
    public function normalize(): array
    {
        return [
            'partition_names' => $this->partitionNames,
            'partition_types' => $this->partitionTypes->normalize(),
            'metadata_columns' => $this->metadataColumns,
        ];
    }

    /**
     * $declared carries the type the read gives a partition column - a withSchema()d or a file footer type;
     * keep() drops the body columns it brings.
     */
    public function partitions(Schema $declared): Schema
    {
        if ($this->partitionNames === []) {
            return new Schema();
        }

        return $this->declare($declared)->keep(...array_keys($this->partitionNames));
    }

    /**
     * Takes the schema declare() already produced, so the values and the schema they are written under
     * can never come from two different declarations.
     */
    public function forFile(SourceFile $source, Schema $declared): FileConstants
    {
        $values = [];

        foreach ($this->partitionNames as $name => $_) {
            $values[$name] = self::partitionValue(
                $declared->get($name),
                $name,
                array_key_exists($name, $source->partitionValues) ? $source->partitionValues[$name] : null,
                $source->path,
            );
        }

        return new FileConstants($this->metadataColumns ? $source->uri() : null, $this->partitionNames, $values);
    }

    /**
     * @return list<string>
     */
    public function tail(): array
    {
        return [
            ...($this->metadataColumns ? ['_input_file_uri'] : []),
            ...array_map(static fn(int|string $name): string => (string) $name, array_keys($this->partitionNames)),
        ];
    }

    /**
     * So a source carrying a partition-named column in its body does not get it typed from the data.
     */
    public function withoutTail(Schema $inferred): Schema
    {
        return $inferred->gracefulRemove(...$this->tail());
    }
}
