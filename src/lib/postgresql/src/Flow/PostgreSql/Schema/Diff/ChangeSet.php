<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

/**
 * @template T
 * @template TDiff
 */
final readonly class ChangeSet
{
    /**
     * @param list<T> $added
     * @param list<T> $removed
     * @param null|list<TDiff> $modified
     * @param null|array<string, T> $renamed
     */
    public function __construct(
        public array $added,
        public array $removed,
        public ?array $modified = null,
        public ?array $renamed = null,
    ) {}

    /**
     * @template TObj
     * @template TObjDiff
     *
     * @param list<TObj> $source
     * @param list<TObj> $target
     * @param callable(TObj): string $identityFn
     * @param null|(callable(TObj, TObj): ?TObjDiff) $modifiedFn
     *
     * @return self<TObj, TObjDiff>
     */
    public static function fromNamedObjects(
        array $source,
        array $target,
        callable $identityFn,
        ?callable $modifiedFn = null,
    ): self {
        $sourceMap = [];

        foreach ($source as $item) {
            $sourceMap[$identityFn($item)] = $item;
        }

        $targetMap = [];

        foreach ($target as $item) {
            $targetMap[$identityFn($item)] = $item;
        }

        $added = [];
        $removed = [];
        $modified = [];

        foreach ($targetMap as $identity => $item) {
            if (!array_key_exists($identity, $sourceMap)) {
                $added[] = $item;
            }
        }

        foreach ($sourceMap as $identity => $item) {
            if (!array_key_exists($identity, $targetMap)) {
                $removed[] = $item;
            }
        }

        if ($modifiedFn !== null) {
            foreach ($sourceMap as $identity => $sourceItem) {
                if (!array_key_exists($identity, $targetMap)) {
                    continue;
                }

                $diff = $modifiedFn($sourceItem, $targetMap[$identity]);

                if ($diff !== null) {
                    $modified[] = $diff;
                }
            }
        }

        return new self($added, $removed, $modified !== [] ? $modified : null);
    }
}
