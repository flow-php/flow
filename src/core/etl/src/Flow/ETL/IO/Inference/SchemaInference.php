<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Inference;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_positive_integer;

final readonly class SchemaInference
{
    /**
     * @param int<1, max>|-1 $sampleSize - rows observed across every sampled source before the schema is frozen; -1 observes all
     * @param int<1, max>|-1 $filesToSniff - sources that yielded a row, counted while the row budget is not spent;
     *                                     -1 sniffs all.
     * @param null|InferredTypes $types - the types inference may produce; null is every type but markup
     * @param bool $unionByName - false takes the column set from the first source and treats a later source's
     *                            unseen column as a divergence; true unions the sets and runs no divergence check
     * @param null|Sniffs $sniffs - runs the whole-unit sniffs of sampleSize(-1); null sniffs in this process; a live
     *                            collaborator, never normalized
     *
     * @throws InvalidArgumentException when $sampleSize or $filesToSniff is below 1 and not -1
     */
    public function __construct(
        public int $sampleSize = 20_480,
        public int $filesToSniff = 10,
        public ?InferredTypes $types = null,
        public bool $unionByName = false,
        public ?Sniffs $sniffs = null,
    ) {
        // @mago-ignore analysis:impossible-condition,redundant-comparison,redundant-logical-operation
        if ($sampleSize < 1 && $sampleSize !== -1) {
            throw new InvalidArgumentException('Sample size must be greater than 0, or -1 for all rows');
        }

        // @mago-ignore analysis:impossible-condition,redundant-comparison,redundant-logical-operation
        if ($filesToSniff < 1 && $filesToSniff !== -1) {
            throw new InvalidArgumentException('Files to sniff must be greater than 0, or -1 for all sources');
        }
    }

    /**
     * @param array<array-key, mixed> $data the shape of normalize()
     */
    public static function fromArray(array $data): self
    {
        $types = type_optional(type_array())->assert($data['types'] ?? null);
        $sampleSize = type_integer()->assert($data['sample_size'] ?? null);
        $filesToSniff = type_integer()->assert($data['files_to_sniff'] ?? null);

        return new self(
            $sampleSize === -1 ? -1 : type_positive_integer()->assert($sampleSize),
            $filesToSniff === -1 ? -1 : type_positive_integer()->assert($filesToSniff),
            $types === null ? null : InferredTypes::fromArray($types),
            type_boolean()->assert($data['union_by_name'] ?? null),
        );
    }

    public function candidates(): InferredTypes
    {
        return $this->types ?? InferredTypes::default();
    }

    /**
     * @return array{sample_size: int, files_to_sniff: int, types: null|list<array<string, mixed>>, union_by_name: bool}
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
