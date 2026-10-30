<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\Filesystem\Path;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_optional;

/**
 * The partition values come from the path, so a read knows them without opening the file.
 */
final readonly class SourceFile
{
    /**
     * @var array<string, null|string>
     */
    public array $partitionValues;

    public function __construct(
        public Path $path,
        public ?int $size = null,
    ) {
        $values = [];

        foreach ($path->partitions() as $partition) {
            $values[$partition->name] = $partition->value;
        }

        $this->partitionValues = $values;
    }

    /**
     * @param array<array-key, mixed> $data the shape of normalize()
     */
    public static function fromArray(array $data): self
    {
        return new self(
            Path::fromArray(type_array()->assert($data['path'] ?? null)),
            type_optional(type_integer())->assert($data['size'] ?? null),
        );
    }

    /**
     * @return array{path: array<string, mixed>, size: ?int}
     */
    public function normalize(): array
    {
        return ['path' => $this->path->normalize(), 'size' => $this->size];
    }

    public function uri(): string
    {
        return $this->path->uri();
    }
}
