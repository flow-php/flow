<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Partition;
use Throwable;

use function array_key_exists;
use function array_values;
use function Flow\ETL\DSL\str_schema;
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
        private PartitionColumns $partitionColumns,
        private array $partitionNames,
        private PartitionTypes $partitionTypes,
        private bool $metadataColumns,
    ) {}

    public function apply(Rows $rows): Rows
    {
        return $this->partitionColumns->apply($rows, $this->partitionNames, $this->partitionTypes);
    }

    public function declare(Schema $schema): Schema
    {
        return $this->partitionColumns->declare(
            $this->metadataColumns ? $schema->add(str_schema('_input_file_uri')) : $schema,
            $this->partitionNames,
            $this->partitionTypes,
        );
    }

    /**
     * Takes the schema declare() already produced, so the values and the schema they are written under
     * can never come from two different declarations.
     */
    public function forFile(SourceFile $source, Schema $declared): FileConstants
    {
        $values = [];

        foreach ($this->partitionNames as $name => $_) {
            $definition = $declared->get($name);
            $value = array_key_exists($name, $source->partitionValues) ? $source->partitionValues[$name] : null;

            if ($definition->matches($value)) {
                $values[$name] = $value;

                continue;
            }

            try {
                $values[$name] = $definition->type()->cast($value);
            } catch (Throwable $e) {
                throw new InvalidArgumentException(
                    sprintf(
                        'Partition column "%s" of file "%s" declares type %s, but its path value "%s" cannot be cast to it.',
                        $name,
                        $source->uri(),
                        $definition->type()->toString(),
                        array_key_exists($name, $source->partitionValues) ? $value ?? Partition::NULL_VALUE : '',
                    ),
                    0,
                    $e,
                );
            }
        }

        return new FileConstants(
            $this->partitionColumns,
            $this->metadataColumns ? $source->uri() : null,
            $this->partitionNames,
            $values,
        );
    }

    /**
     * @return list<string>
     */
    public function tail(): array
    {
        return array_values($this->declare(new Schema())->references()->names());
    }

    /**
     * So a source carrying a partition-named column in its body does not get it typed from the data.
     */
    public function withoutTail(Schema $inferred): Schema
    {
        return $inferred->gracefulRemove(...$this->tail());
    }
}
