<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Explain;

use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Outputs;
use SplObjectStorage;

use function array_map;
use function implode;

/**
 * The plan in the direction rows move: each source first, every node followed by the nodes that read it. Numbers
 * are the tree's, so one node carries one number in every format. Outputs only groups the plan's consumers, so it
 * is left out - its children already end the flow.
 */
final readonly class FlowLayout implements Layout
{
    public function __construct(
        private TreeLayout $tree = new TreeLayout(),
    ) {}

    public function render(Entry $root): string
    {
        /** @var SplObjectStorage<Node, list<Entry>> $readers */
        $readers = new SplObjectStorage();
        $sources = [];
        $this->collect($root, $readers, $sources);

        /** @var SplObjectStorage<Node, true> $placed */
        $placed = new SplObjectStorage();

        return implode("\n", array_map(fn(Entry $source): string => $this->tree->render($this->reversed(
            $source,
            $readers,
            $placed,
        )), $sources));
    }

    /**
     * @param SplObjectStorage<Node, list<Entry>> $readers every node's readers, in the order the tree visits them
     * @param list<Entry> $sources the entries without inputs, in the order the tree visits them
     */
    public function collect(Entry $entry, SplObjectStorage $readers, array &$sources): void
    {
        if ($entry->shared) {
            return;
        }

        if ($entry->children === []) {
            $sources[] = $entry;
        }

        foreach ($entry->children as $child) {
            $readers[$child->node] = [...($readers->offsetExists($child->node) ? $readers[$child->node] : []), $entry];
            $this->collect($child, $readers, $sources);
        }
    }

    /**
     * @param SplObjectStorage<Node, list<Entry>> $readers
     * @param SplObjectStorage<Node, true> $placed the nodes already printed, any later reach is a shared reference
     */
    public function reversed(Entry $entry, SplObjectStorage $readers, SplObjectStorage $placed): Entry
    {
        $placed[$entry->node] = true;
        $children = [];

        foreach ($readers->offsetExists($entry->node) ? $readers[$entry->node] : [] as $reader) {
            if ($reader->node instanceof Outputs) {
                continue;
            }

            $children[] = $placed->offsetExists($reader->node)
                ? new Entry($reader->node, $reader->number, true, [])
                : $this->reversed($reader, $readers, $placed);
        }

        return new Entry($entry->node, $entry->number, false, $children);
    }
}
