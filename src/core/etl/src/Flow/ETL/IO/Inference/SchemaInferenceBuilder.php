<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Inference;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\Types\Type;

use function Flow\Types\DSL\type_string;

final class SchemaInferenceBuilder
{
    /**
     * @var null|int<1, max>
     */
    private ?int $filesToSniff;

    /**
     * @var null|int<1, max>
     */
    private ?int $sampleSize;

    private ?InferredTypes $types;

    private ?Sniffs $sniffs;

    private bool $unionByName;

    public function __construct()
    {
        $defaults = new SchemaInference();
        $this->filesToSniff = $defaults->filesToSniff;
        $this->sampleSize = $defaults->sampleSize;
        $this->types = $defaults->types;
        $this->unionByName = $defaults->unionByName;
        $this->sniffs = $defaults->sniffs;
    }

    public function allStrings(): self
    {
        return $this->types(type_string());
    }

    public function build(): SchemaInference
    {
        return new SchemaInference(
            $this->sampleSize,
            $this->filesToSniff,
            $this->types,
            $this->unionByName,
            $this->sniffs,
        );
    }

    public function everyRow(): self
    {
        $this->sampleSize = null;

        return $this;
    }

    /**
     * Leading row-less sources are all opened. Ignored by a source that lists no files.
     */
    public function everySource(): self
    {
        $this->filesToSniff = null;

        return $this;
    }

    /**
     * @param int<1, max> $filesToSniff - sources that yielded a row. Leading row-less sources are all opened.
     *                                  Ignored by a source that lists no files
     */
    public function filesToSniff(int $filesToSniff): self
    {
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($filesToSniff < 1) {
            throw new InvalidArgumentException(
                'Files to sniff must be greater than 0. Use everySource() to infer from every source.',
            );
        }

        $this->filesToSniff = $filesToSniff;

        return $this;
    }

    /**
     * @param int<1, max> $sampleSize
     */
    public function sampleSize(int $sampleSize): self
    {
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($sampleSize < 1) {
            throw new InvalidArgumentException(
                'Sample size must be greater than 0. Use everyRow() to infer from every row.',
            );
        }

        $this->sampleSize = $sampleSize;

        return $this;
    }

    /**
     * Runs the whole-unit sniffs of everyRow(), for example through a process launcher to sniff in parallel.
     */
    public function sniffWith(Sniffs $sniffs): self
    {
        $this->sniffs = $sniffs;

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
