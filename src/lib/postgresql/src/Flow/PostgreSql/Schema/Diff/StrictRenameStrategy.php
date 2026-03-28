<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

final readonly class StrictRenameStrategy implements RenameStrategy
{
    public function resolve(array $candidates) : array
    {
        $candidatesByAdded = [];

        foreach ($candidates as $candidate) {
            $candidatesByAdded[$candidate->addedName][] = $candidate->removedName;
        }

        $matched = [];
        $matchedRemoved = [];

        foreach ($candidatesByAdded as $addedName => $removedCandidates) {
            if (\count($removedCandidates) !== 1) {
                continue;
            }

            $removedName = $removedCandidates[0];

            if (array_key_exists($removedName, $matchedRemoved)) {
                continue;
            }

            $matched[] = new RenameMatch($addedName, $removedName);
            $matchedRemoved[$removedName] = true;
        }

        return $matched;
    }
}
