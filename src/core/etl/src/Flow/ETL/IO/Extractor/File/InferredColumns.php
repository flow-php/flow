<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\IO\Inference\SchemaInference;
use Flow\ETL\Values\Schema;

use function array_diff;
use function array_unique;
use function array_values;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

/**
 * The columns an inferred schema was derived from, which every file a grid format reads must carry.
 */
final readonly class InferredColumns
{
    /**
     * @param Schema $inferred the inferred body, file constant columns excluded
     * @param list<string> $tail the file constant columns, never part of a file's own columns
     * @param string $inferredFrom the source the inferred header was read from
     */
    public function __construct(
        private Schema $inferred,
        private array $tail,
        private SchemaInference $inference,
        private string $inferredFrom,
    ) {}

    /**
     * @param array<array-key, mixed> $data the shape of normalize()
     */
    public static function fromArray(array $data): self
    {
        return new self(
            Schema::fromArray(type_array()->assert($data['inferred'] ?? null)),
            type_list(type_string())->assert($data['tail'] ?? null),
            SchemaInference::fromArray(type_array()->assert($data['inference'] ?? null)),
            type_string()->assert($data['inferred_from'] ?? null),
        );
    }

    /**
     * @return array{inferred: array<array-key, array<mixed>>, tail: list<string>, inference: array<string, mixed>, inferred_from: string}
     */
    public function normalize(): array
    {
        return [
            'inferred' => $this->inferred->normalize(),
            'tail' => $this->tail,
            'inference' => $this->inference->normalize(),
            'inferred_from' => $this->inferredFrom,
        ];
    }

    /**
     * Refuses a file whose own columns (header) are not the inferred ones; a file with no columns passes.
     *
     * @param list<string> $columns the file's columns, constant columns and duplicates allowed
     *
     * @throws InferredSchemaException
     */
    public function refuseDivergence(string $source, array $columns): void
    {
        $expected = $this->inferred->names();
        $columns = array_values(array_diff(array_unique($columns), $this->tail));

        if ($columns !== [] && (array_diff($columns, $expected) !== [] || array_diff($expected, $columns) !== [])) {
            throw InferredSchemaException::columnsDiverge(
                $source,
                $this->inferredFrom,
                $expected,
                $columns,
                $this->inference->sampleSize,
                $this->inference->filesToSniff,
            );
        }
    }
}
