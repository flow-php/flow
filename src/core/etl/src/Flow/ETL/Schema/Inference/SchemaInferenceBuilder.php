<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Inference;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Types\Type;

use function Flow\Types\DSL\type_string;

final class SchemaInferenceBuilder
{
    /**
     * @var int<1, max>|-1
     */
    private int $filesToSniff = 10;

    /**
     * @var int<1, max>|-1
     */
    private int $sampleSize = 20_480;

    private ?InferredTypes $types = null;

    private bool $unionByName = false;

    public function allStrings(): self
    {
        return $this->types(type_string());
    }

    public function build(): SchemaInference
    {
        return new SchemaInference($this->sampleSize, $this->filesToSniff, $this->types, $this->unionByName);
    }

    /**
     * @param int<1, max>|-1 $filesToSniff - sources that yielded a row; -1 sniffs every source. Leading row-less
     *                                     sources are all opened. Ignored by a source that lists no files
     */
    public function filesToSniff(int $filesToSniff): self
    {
        // @mago-ignore analysis:impossible-condition,redundant-comparison,redundant-logical-operation
        if ($filesToSniff < 1 && $filesToSniff !== -1) {
            throw new InvalidArgumentException('Files to sniff must be greater than 0, or -1 for all sources');
        }

        $this->filesToSniff = $filesToSniff;

        return $this;
    }

    /**
     * @param int<1, max>|-1 $sampleSize - -1 observes every row
     */
    public function sampleSize(int $sampleSize): self
    {
        // @mago-ignore analysis:impossible-condition,redundant-comparison,redundant-logical-operation
        if ($sampleSize < 1 && $sampleSize !== -1) {
            throw new InvalidArgumentException('Sample size must be greater than 0, or -1 for all rows');
        }

        $this->sampleSize = $sampleSize;

        return $this;
    }

    /**
     * @param Type<mixed> ...$types - order is irrelevant and string is always admitted; a rung outside this set
     *                                never runs, so a narrower set is also a cheaper one
     */
    public function types(Type ...$types): self
    {
        $this->types = new InferredTypes(...$types);

        return $this;
    }

    public function unionByName(bool $unionByName = true): self
    {
        $this->unionByName = $unionByName;

        return $this;
    }
}
