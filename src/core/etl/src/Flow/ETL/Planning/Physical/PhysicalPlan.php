<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Physical;

use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\IO\Extractor\Refreshable;
use Flow\ETL\Values\Schema;

use function array_values;

/**
 * Enumerates the spine's pipelines only - a branch's pipeline hangs off the BranchPipeline that holds it.
 */
final readonly class PhysicalPlan
{
    /**
     * @param null|Schema $schema the rows the plan returns; null when $refusal says why it is not derivable
     * @param null|SchemaNotDerivableException $refusal why the plan is not fully described; a branch's refusal leaves
     *                                                  $schema set
     * @param bool $countsOnly the plan's root is a Write: nothing reads the returned rows' columns
     * @param list<Refreshable> $refreshedAfterRun the sources whose listing and inferred schema last until the run
     *                                             ends
     *
     * @throws InvalidLogicException when $schema and $refusal are both null
     */
    public function __construct(
        private Pipeline $root,
        private ?Schema $schema,
        private ?SchemaNotDerivableException $refusal = null,
        private bool $countsOnly = false,
        private array $refreshedAfterRun = [],
    ) {
        if ($schema === null && $refusal === null) {
            throw InvalidLogicException::because('A plan without a schema requires a refusal');
        }
    }

    public function root(): Pipeline
    {
        return $this->root;
    }

    /**
     * Nothing reads the columns of the rows the plan returns: its root is a Write, so a run only counts them.
     */
    public function countsOnly(): bool
    {
        return $this->countsOnly;
    }

    /**
     * @return non-empty-list<Pipeline> every pipeline of the spine, leaf first
     */
    public function pipelines(): array
    {
        return $this->root->chain();
    }

    public function refreshingAfterRun(Refreshable ...$sources): self
    {
        return new self($this->root, $this->schema, $this->refusal, $this->countsOnly, array_values($sources));
    }

    /**
     * Drops what the sources listed and inferred for this run, so the next run reads them as they are then.
     */
    public function refreshSources(): void
    {
        foreach ($this->refreshedAfterRun as $source) {
            $source->refresh();
        }
    }

    /**
     * @throws SchemaNotDerivableException
     */
    public function schema(): Schema
    {
        return (
            $this->schema ?? throw $this->refusal ?? InvalidLogicException::because(
                'A plan without a schema requires a refusal',
            )
        );
    }

    public function refusal(): ?SchemaNotDerivableException
    {
        return $this->refusal;
    }

    public function described(): bool
    {
        return $this->refusal === null;
    }
}
