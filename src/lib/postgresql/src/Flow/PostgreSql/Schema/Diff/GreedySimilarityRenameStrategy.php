<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

final readonly class GreedySimilarityRenameStrategy implements RenameStrategy
{
    public function __construct(
        private SimilarityStrategy $similarity,
    ) {
    }

    public function resolve(array $candidates) : array
    {
        $candidatesByAdded = [];

        foreach ($candidates as $candidate) {
            $candidatesByAdded[$candidate->addedName][] = $candidate->removedName;
        }

        $matched = [];
        $matchedAdded = [];
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
            $matchedAdded[$addedName] = true;
            $matchedRemoved[$removedName] = true;
        }

        $pairs = [];

        foreach ($candidates as $candidate) {
            if (array_key_exists($candidate->addedName, $matchedAdded) || array_key_exists($candidate->removedName, $matchedRemoved)) {
                continue;
            }

            $score = $this->similarity->similarity($candidate->addedName, $candidate->removedName);

            if ($score >= $this->similarity->threshold()) {
                $pairs[] = ['added' => $candidate->addedName, 'removed' => $candidate->removedName, 'score' => $score];
            }
        }

        \usort($pairs, static fn (array $a, array $b) : int => $b['score'] <=> $a['score']);

        foreach ($pairs as $pair) {
            if (array_key_exists($pair['added'], $matchedAdded) || array_key_exists($pair['removed'], $matchedRemoved)) {
                continue;
            }

            $matched[] = new RenameMatch($pair['added'], $pair['removed']);
            $matchedAdded[$pair['added']] = true;
            $matchedRemoved[$pair['removed']] = true;
        }

        return $matched;
    }
}
