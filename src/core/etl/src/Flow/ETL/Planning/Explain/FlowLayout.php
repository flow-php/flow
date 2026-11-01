<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Explain;

use Flow\ETL\Planning\Logical\Node\Outputs;
use SplObjectStorage;

use function array_map;
use function implode;

/**
 * Sources first, each node followed by its readers; Outputs is left out.
 */
final readonly class FlowLayout implements Layout
{
    public function __construct(
        private TreeLayout $tree = new TreeLayout(),
    ) {}

    public function render(RenderedNode $root): string
    {
        /** @var SplObjectStorage<object, list<RenderedNode>> $readers */
        $readers = new SplObjectStorage();
        $sources = [];
        $this->collect($root, $readers, $sources);

        /** @var SplObjectStorage<object, true> $placed */
        $placed = new SplObjectStorage();

        return implode("\n", array_map(fn(RenderedNode $source): string => $this->tree->render($this->reversed(
            $source,
            $readers,
            $placed,
        )), $sources));
    }

    /**
     * @param SplObjectStorage<object, list<RenderedNode>> $readers every node's readers, in the order the tree visits them
     * @param list<RenderedNode> $sources the entries without inputs, in the order the tree visits them
     */
    public function collect(RenderedNode $entry, SplObjectStorage $readers, array &$sources): void
    {
        if ($entry->shared) {
            return;
        }

        if ($entry->children === []) {
            $sources[] = $entry;
        }

        foreach ($entry->children as $child) {
            $readers[$child->source] = [
                ...($readers->offsetExists($child->source) ? $readers[$child->source] : []),
                $entry,
            ];
            $this->collect($child, $readers, $sources);
        }
    }

    /**
     * @param SplObjectStorage<object, list<RenderedNode>> $readers
     * @param SplObjectStorage<object, true> $placed the nodes already printed, any later reach is a shared reference
     */
    public function reversed(RenderedNode $entry, SplObjectStorage $readers, SplObjectStorage $placed): RenderedNode
    {
        $placed[$entry->source] = true;
        $children = [];

        foreach ($readers->offsetExists($entry->source) ? $readers[$entry->source] : [] as $reader) {
            if ($reader->source instanceof Outputs) {
                continue;
            }

            $children[] = $placed->offsetExists($reader->source)
                ? $reader->with([], shared: true)
                : $this->reversed($reader, $readers, $placed);
        }

        return $entry->with($children);
    }
}
