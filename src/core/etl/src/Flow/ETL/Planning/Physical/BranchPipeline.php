<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Physical;

use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\IO\Loader;
use Flow\ETL\Values\Schema;

final readonly class BranchPipeline
{
    /**
     * @param Pipeline $pipeline the last pipeline of the branch; the first one of its chain is fed batch by batch
     * @param Schema $input the rows the branch is fed
     */
    public function __construct(
        public Pipeline $pipeline,
        public Schema $input,
    ) {}

    /**
     * The writes the branch ends in: the loaders and branches its last pipeline ends with.
     *
     * @throws InvalidLogicException when the branch ends in no write
     *
     * @return non-empty-list<Loader|BranchPipeline|TransactionGroup>
     */
    public function writes(): array
    {
        $writes = [];

        foreach ($this->pipeline->steps->steps as $step) {
            $writes =
                $step instanceof Loader || $step instanceof self || $step instanceof TransactionGroup
                    ? [...$writes, $step]
                    : [];
        }

        if ($writes === []) {
            throw InvalidLogicException::because('A branch must end in a write');
        }

        return $writes;
    }
}
