<?php

declare(strict_types=1);

namespace Flow\ETL\Planner;

use Flow\ETL\ErrorHandler;
use Flow\ETL\ErrorHandler\ThrowError;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Executor\SinkOffers;
use Flow\ETL\Executor\TransactionalSinks;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Transaction;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Plan\Sinks;
use SplObjectStorage;

use function array_key_exists;
use function array_key_last;
use function array_reverse;
use function array_slice;
use function count;
use function get_debug_type;

/**
 * Attaches a plan's sink roots to the spine: the step each spine node gains for them. attach() is the entry;
 * the other methods are the walk it runs. SinkFeedFactory builds the pipelines the walk hands rows to.
 *
 * @type Leaf = array{write: Write, loader: Loader, path: list<Node>, transaction: null|Transaction}
 */
final class SinkAttachment
{
    private int $next = 0;

    private readonly SinkFeedFactory $feed;

    public function __construct(
        private readonly PlannedNodes $planned,
        private readonly FlowContext $context,
    ) {
        $this->feed = new SinkFeedFactory($planned, $context);
    }

    /**
     * Every sink root attached at the spine node its chain meets, as the Loader steps that node's segment gains.
     * Side pipelines take the ids first; next() is where the spine's own cuts continue.
     *
     * @param SplObjectStorage<Node, Node> $onSpine
     *
     * @throws InvalidLogicException when a sink shares no node with the spine, a Write does not translate to a Loader,
     *                               or the sinks of one transaction attach to different nodes
     *
     * @return SplObjectStorage<Node, list<Loader>>
     */
    public function attach(Sinks $sinks, SplObjectStorage $onSpine): SplObjectStorage
    {
        /** @var SplObjectStorage<Node, non-empty-list<Leaf>> $attached */
        $attached = new SplObjectStorage();

        foreach ($sinks as $sink) {
            $at = null;

            foreach ($sink instanceof Transaction ? $sink->children() : [$sink] as $write) {
                $chain = $this->chain($write, $onSpine);
                $bottom = $chain[array_key_last($chain)]->children()[0];

                if ($at !== null && $bottom !== $at) {
                    throw InvalidLogicException::because('Every sink of one transaction must attach to the same node');
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
                        'transaction' => $sink instanceof Transaction ? $sink : null,
                    ],
                ];
            }
        }

        /** @var SplObjectStorage<Node, list<Loader>> $remembered */
        $remembered = new SplObjectStorage();

        foreach ($attached as $at) {
            $remembered[$at] = $this->consumers($attached[$at], $at, $this->context->errorHandler());
        }

        return $remembered;
    }

    public function next(): int
    {
        return $this->next;
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
                throw InvalidLogicException::sinkNotOnSpine($write->loader::class);
            }
        }

        return $chain;
    }

    /**
     * A node several sinks share feeds them ONCE - it is the point their rows fan out - and a transaction attaches
     * where its children part, so a node they all share runs before it opens.
     *
     * @param non-empty-list<Leaf> $leaves the sinks hanging off $host, each with the nodes between $host and its Write
     *
     * @throws InvalidLogicException when a sink outside a transaction shares a node with one of its children
     *
     * @return list<Loader>
     */
    public function consumers(array $leaves, Node $host, ErrorHandler $handler): array
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
                $steps[] = $this->consumer($group, $host, $handler);

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
                        'A sink outside a transaction cannot share a node with one of its children',
                    );
                }

                $children[] = $this->consumer($mine, $host, new ThrowError());
            }

            $steps[] = new TransactionalSinks($opening->transaction, $children);
        }

        return $steps;
    }

    /**
     * @param non-empty-list<Leaf> $group sinks whose nodes above $host start with the same node
     */
    public function consumer(array $group, Node $host, ErrorHandler $handler): Loader
    {
        if (count($group) === 1) {
            $leaf = $group[0];

            return (
                $leaf['path'] === []
                    ? $leaf['loader']
                    : $this->feed->of($leaf['path'], [$leaf['loader']], $host, new SinkOffers($handler), $this->next++)
            );
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

        $sharedId = $this->next++;
        $offers = new SinkOffers($handler);

        return $this->feed->of(
            $shared,
            $this->consumers($rest, $shared[array_key_last($shared)], $offers),
            $host,
            $offers,
            $sharedId,
        );
    }
}
