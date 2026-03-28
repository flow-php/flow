<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

interface RenameStrategy
{
    /**
     * Given structurally matching candidate pairs (added name → removed name),
     * determine which pairs are renames.
     *
     * @param list<RenameCandidate> $candidates
     *
     * @return list<RenameMatch>
     */
    public function resolve(array $candidates) : array;
}
