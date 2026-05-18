<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use Flow\PostgreSql\Schema\Index;

use function array_values;

final readonly class IndexComparator
{
    public function __construct(
        private RenameStrategy $renameStrategy,
    ) {}

    /**
     * @param list<Index> $sourceIndexes
     * @param list<Index> $targetIndexes
     *
     * @return ChangeSet<Index, mixed>
     */
    public function compare(array $sourceIndexes, array $targetIndexes): ChangeSet
    {
        $sourceMap = [];

        foreach ($sourceIndexes as $idx) {
            $sourceMap[$idx->name] = $idx;
        }

        $targetMap = [];

        foreach ($targetIndexes as $idx) {
            $targetMap[$idx->name] = $idx;
        }

        $added = [];
        $removed = [];

        foreach ($targetMap as $name => $idx) {
            if (!array_key_exists($name, $sourceMap)) {
                $added[] = $idx;
            } elseif (!$sourceMap[$name]->isEqualStructure($idx)) {
                $removed[] = $sourceMap[$name];
                $added[] = $idx;
            }
        }

        foreach ($sourceMap as $name => $idx) {
            if (!array_key_exists($name, $targetMap)) {
                $removed[] = $idx;
            }
        }

        $renameResult = $this->detectIndexRenames($added, $removed);

        return new ChangeSet($renameResult->added, $renameResult->removed, renamed: $renameResult->renamed ?? []);
    }

    /**
     * @param list<Index> $added
     * @param list<Index> $removed
     *
     * @return ChangeSet<Index, mixed>
     */
    private function detectIndexRenames(array $added, array $removed): ChangeSet
    {
        $addedMap = [];

        foreach ($added as $idx) {
            $addedMap[$idx->name] = $idx;
        }

        $removedMap = [];

        foreach ($removed as $idx) {
            $removedMap[$idx->name] = $idx;
        }

        $candidates = [];

        foreach ($addedMap as $addedName => $addedIdx) {
            foreach ($removedMap as $removedName => $removedIdx) {
                if ($addedIdx->isEqualStructure($removedIdx)) {
                    $candidates[] = new RenameCandidate($addedName, $removedName);
                }
            }
        }

        $renamed = [];

        foreach ($this->renameStrategy->resolve($candidates) as $match) {
            $renamed[$match->removedName] = $addedMap[$match->addedName];
            unset($addedMap[$match->addedName], $removedMap[$match->removedName]);
        }

        return new ChangeSet(
            array_values($addedMap),
            array_values($removedMap),
            renamed: $renamed !== [] ? $renamed : null,
        );
    }
}
