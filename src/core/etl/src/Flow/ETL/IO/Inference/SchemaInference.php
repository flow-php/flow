<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Inference;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_positive_integer;

final readonly class SchemaInference
{
    /**
     * @param null|int<1, max> $sampleSize - rows observed across every sampled source before the schema is frozen;
     *                                     null observes every row, see everyRow()
     * @param null|int<1, max> $filesToSniff - sources that yielded a row, counted while the row budget is not spent;
     *                                       null sniffs every source, see everySource()
     * @param null|InferredTypes $types - the types inference may produce; null is every type but markup
     * @param bool $unionByName - false takes the column set from the first source and treats a later source's
     *                            unseen column as a divergence; true unions the sets and runs no divergence check
     * @param null|Sniffs $sniffs - runs the whole-unit sniffs of everyRow(); null sniffs in this process; a live
     *                            collaborator, never normalized
     *
     * @throws InvalidArgumentException when $sampleSize or $filesToSniff is below 1
     */
    public function __construct(
        public ?int $sampleSize = 20_480,
        public ?int $filesToSniff = 10,
        public ?InferredTypes $types = null,
        public bool $unionByName = false,
        public ?Sniffs $sniffs = null,
    ) {
        // @mago-ignore analysis:impossible-condition,redundant-comparison,redundant-logical-operation
        if ($sampleSize !== null && $sampleSize < 1) {
            throw new InvalidArgumentException(
                'Sample size must be greater than 0. Use everyRow() to infer from every row.',
            );
        }

        // @mago-ignore analysis:impossible-condition,redundant-comparison,redundant-logical-operation
        if ($filesToSniff !== null && $filesToSniff < 1) {
            throw new InvalidArgumentException(
                'Files to sniff must be greater than 0. Use everySource() to infer from every source.',
            );
        }
    }

    /**
     * @param array<array-key, mixed> $data the shape of normalize()
     */
    public static function fromArray(array $data): self
    {
        $types = type_optional(type_array())->assert($data['types'] ?? null);

        return new self(
            type_optional(type_positive_integer())->assert($data['sample_size'] ?? null),
            type_optional(type_positive_integer())->assert($data['files_to_sniff'] ?? null),
            $types === null ? null : InferredTypes::fromArray($types),
            type_boolean()->assert($data['union_by_name'] ?? null),
        );
    }

    public function everyRow(): self
    {
        return new self(null, $this->filesToSniff, $this->types, $this->unionByName, $this->sniffs);
    }

    public function everySource(): self
    {
        return new self($this->sampleSize, null, $this->types, $this->unionByName, $this->sniffs);
    }

    public function candidates(): InferredTypes
    {
        return $this->types ?? InferredTypes::default();
    }

    /**
     * @return array{sample_size: null|int, files_to_sniff: null|int, types: null|list<array<string, mixed>>, union_by_name: bool}
     */
    public function normalize(): array
    {
        return [
            'sample_size' => $this->sampleSize,
            'files_to_sniff' => $this->filesToSniff,
            'types' => $this->types?->normalize(),
            'union_by_name' => $this->unionByName,
        ];
    }
}
