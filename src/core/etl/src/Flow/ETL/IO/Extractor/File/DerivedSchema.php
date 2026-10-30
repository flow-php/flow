<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Validator\StrictValidator;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_string;

/**
 * The schema a self-describing format derived from its first file, which every file it reads must match.
 */
final readonly class DerivedSchema
{
    /**
     * @param string $derivedFrom the file the schema was read from
     */
    public function __construct(
        private Schema $derived,
        private string $derivedFrom,
    ) {}

    /**
     * @param array<array-key, mixed> $data the shape of normalize()
     */
    public static function fromArray(array $data): self
    {
        return new self(
            Schema::fromArray(type_array()->assert($data['derived'] ?? null)),
            type_string()->assert($data['derived_from'] ?? null),
        );
    }

    /**
     * @return array{derived: array<array-key, array<mixed>>, derived_from: string}
     */
    public function normalize(): array
    {
        return ['derived' => $this->derived->normalize(), 'derived_from' => $this->derivedFrom];
    }

    /**
     * @throws InferredSchemaException
     */
    public function refuseDivergence(string $source, Schema $schema): void
    {
        $validation = (new StrictValidator())->validate($this->derived, $schema);

        if (!$validation->isValid()) {
            throw InferredSchemaException::filesDiverge($source, $this->derivedFrom, $validation->toString());
        }
    }
}
