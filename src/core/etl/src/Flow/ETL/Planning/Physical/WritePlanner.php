<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Physical;

use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\IO\Loader;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Node\Transaction;
use Flow\ETL\Planning\Logical\Node\Write;
use Flow\ETL\Planning\Logical\Writes;
use Flow\ETL\Values\Schema;
use SplObjectStorage;

use function array_key_exists;
use function array_key_last;
use function array_reverse;
use function array_slice;
use function count;
use function get_debug_type;

/**
 * Attaches a plan's write roots to the spine: the step each spine node gains for them. attach() is the entry;
 * the other methods are the walk it runs. A write with steps of its own becomes a branch: the pipelines the runtime
 * feeds batch by batch.
 *
 * @type Leaf = array{write: Write, loader: Loader, path: list<Node>, transaction: null|Transaction}
 */
final class WritePlanner
{
    private int $next = 0;

    public function __construct(
        private readonly PlannedNodes $planned,
    ) {}

    /**
     * Every write root attached at the spine node its chain meets, as the steps that node's pipeline gains.
     * Branch pipelines take the ids first; next() is where the spine's own cuts continue.
     *
     * @param SplObjectStorage<Node, Node> $onSpine
     *
     * @throws InvalidLogicException when a write shares no node with the spine, a Write does not translate to a Loader,
     *                               or the writes of one transaction attach to different nodes
     *
     * @return SplObjectStorage<Node, list<Loader|BranchPipeline|TransactionGroup>>
     */
    public function attach(Writes $writes, SplObjectStorage $onSpine): SplObjectStorage
    {
        /** @var SplObjectStorage<Node, non-empty-list<Leaf>> $attached */
        $attached = new SplObjectStorage();

        foreach ($writes as $root) {
            $at = null;

            foreach ($root instanceof Transaction ? $root->children() : [$root] as $write) {
                $chain = $this->chain($write, $onSpine);
                $bottom = $chain[array_key_last($chain)]->children()[0];

                if ($at !== null && $bottom !== $at) {
                    throw InvalidLogicException::because('Every write of one transaction must attach to the same node');
                }

                $at = $bottom;
                $loader = $this->planned->steps($write)[0] ?? null;

                if (!$loader instanceof Loader) {
                    throw InvalidLogicException::because(
                        'A Write must translate to a Loader, %s given',
                        get_debug_type($loader),
                    );
                }

                $attached[$bottom] = [
                    ...($attached->offsetExists($bottom) ? $attached[$bottom] : []),
                    [
                        'write' => $write,
                        'loader' => $loader,
                        'path' => array_reverse(array_slice($chain, 1)),
                        'transaction' => $root instanceof Transaction ? $root : null,
                    ],
                ];
            }
        }

        /** @var SplObjectStorage<Node, list<Loader|BranchPipeline|TransactionGroup>> $remembered */
        $remembered = new SplObjectStorage();

        foreach ($attached as $at) {
            $remembered[$at] = $this->writes($attached[$at], $at);
        }

        return $remembered;
    }

    /**
     * $write first, then every node below it, stopping BEFORE the first node on the spine - so a bare Write sitting
     * directly on a spine node is exactly [$write], and the node it attaches to is never part of its chain.
     *
     * @param SplObjectStorage<Node, Node> $onSpine
     *
     * @throws InvalidLogicException when the chain ends without reaching the spine
     *
     * @return non-empty-list<Node>
     */
    public function chain(Write $write, SplObjectStorage $onSpine): array
    {
        $chain = [$write];

        for ($node = $write->children()[0]; !$onSpine->offsetExists($node); $node = $node->children()[0]) {
            $chain[] = $node;

            if ($node->children() === []) {
                throw InvalidLogicException::writeNotOnSpine($write->loader::class);
            }
        }

        return $chain;
    }

    /**
     * A node several writes share feeds them ONCE - it is the point their rows fan out - and a transaction attaches
     * where its children part, so a node they all share runs before it opens.
     *
     * @param non-empty-list<Leaf> $leaves the writes hanging off $host, each with the nodes between $host and its Write
     *
     * @throws InvalidLogicException when a write outside a transaction shares a node with one of its children
     *
     * @return list<Loader|BranchPipeline|TransactionGroup>
     */
    public function writes(array $leaves, Node $host): array
    {
        /** @var SplObjectStorage<Node, int> $position */
        $position = new SplObjectStorage();
        /** @var list<non-empty-list<Leaf>> $groups */
        $groups = [];
        /** @var SplObjectStorage<Transaction, int> $members */
        $members = new SplObjectStorage();

        foreach ($leaves as $leaf) {
            $first = $leaf['path'][0] ?? $leaf['write'];

            if ($position->offsetExists($first)) {
                $groups[$position[$first]][] = $leaf;
            } else {
                $position[$first] = count($groups);
                $groups[] = [$leaf];
            }

            $transaction = $leaf['transaction'];

            if ($transaction !== null) {
                $members[$transaction] = ($members->offsetExists($transaction) ? $members[$transaction] : 0) + 1;
            }
        }

        /** @var SplObjectStorage<Transaction, Transaction> $opensHere */
        $opensHere = new SplObjectStorage();

        foreach ($groups as $group) {
            /** @var SplObjectStorage<Transaction, int> $inGroup */
            $inGroup = new SplObjectStorage();

            foreach ($group as $leaf) {
                $transaction = $leaf['transaction'];

                if ($transaction !== null) {
                    $inGroup[$transaction] = ($inGroup->offsetExists($transaction) ? $inGroup[$transaction] : 0) + 1;
                }
            }

            foreach ($inGroup as $transaction) {
                if (count($group) === 1 || $inGroup[$transaction] !== $members[$transaction]) {
                    $opensHere[$transaction] = $transaction;
                }
            }
        }

        $steps = [];
        /** @var SplObjectStorage<Transaction, Transaction> $placed */
        $placed = new SplObjectStorage();

        foreach ($groups as $group) {
            $opening = null;

            foreach ($group as $leaf) {
                if ($leaf['transaction'] !== null && $opensHere->offsetExists($leaf['transaction'])) {
                    $opening = $leaf['transaction'];

                    break;
                }
            }

            if ($opening === null) {
                $steps[] = $this->write($group, $host);

                continue;
            }

            if ($placed->offsetExists($opening)) {
                continue;
            }

            $placed[$opening] = $opening;
            $children = [];

            foreach ($groups as $candidate) {
                $mine = [];

                foreach ($candidate as $leaf) {
                    if ($leaf['transaction'] === $opening) {
                        $mine[] = [...$leaf, 'transaction' => null];
                    }
                }

                if ($mine === []) {
                    continue;
                }

                if (count($mine) !== count($candidate)) {
                    throw InvalidLogicException::because(
                        'A write outside a transaction cannot share a node with one of its children',
                    );
                }

                $children[] = $this->write($mine, $host);
            }

            $steps[] = new TransactionGroup($opening->transaction, $children);
        }

        return $steps;
    }

    /**
     * @param non-empty-list<Leaf> $group writes whose nodes above $host start with the same node
     */
    public function write(array $group, Node $host): Loader|BranchPipeline
    {
        if (count($group) === 1) {
            $leaf = $group[0];

            return $leaf['path'] === [] ? $leaf['loader'] : $this->branch($leaf['path'], [$leaf['loader']], $host);
        }

        $shared = [$group[0]['path'][0] ?? $group[0]['write']];

        for ($depth = 1; array_key_exists($depth, $group[0]['path']); $depth++) {
            $node = $group[0]['path'][$depth];

            foreach ($group as $leaf) {
                if (($leaf['path'][$depth] ?? null) !== $node) {
                    break 2;
                }
            }

            $shared[] = $node;
        }

        $rest = [];

        foreach ($group as $leaf) {
            $rest[] = [...$leaf, 'path' => array_slice($leaf['path'], count($shared))];
        }

        return $this->branch($shared, $this->writes($rest, $shared[array_key_last($shared)]), $host);
    }

    /**
     * The branch fed from $host: the steps of $nodes, then $tail, cut at every breaker, numbered from next() on.
     *
     * @param list<Node> $nodes what runs before $tail, bottom-up
     * @param list<Loader|BranchPipeline|TransactionGroup> $tail
     */
    public function branch(array $nodes, array $tail, Node $host): BranchPipeline
    {
        $steps = [];

        foreach ($nodes as $node) {
            foreach ($this->planned->steps($node) as $step) {
                $steps[] = $step;
            }
        }

        $pipelines = Planner::cut(null, [...$steps, ...$tail], $this->next);
        $this->next += count($pipelines);

        return new BranchPipeline($pipelines[count($pipelines) - 1], $this->planned->of($host)->schema ?? new Schema());
    }

    /**
     * The id the next pipeline built here takes; the spine's pipelines continue from it.
     */
    public function next(): int
    {
        return $this->next;
    }
}
