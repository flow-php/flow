<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Physical;

use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\Foundation\Progress;
use Flow\ETL\Foundation\ProgressStages;
use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\VoidProgress;
use Flow\ETL\IO\Extractor\File\FileExtractor;
use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Loader\File\FileCommit;
use Flow\ETL\IO\Loader\File\FileLoader;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Planning\Logical\Node;
use SplObjectStorage;

/**
 * What one plan - a frame's, or a join's right side - has planned so far: every node by identity, and the first
 * schema refusal.
 */
final class PlannedNodes
{
    /**
     * @var SplObjectStorage<Node, PlannedNode>
     */
    private SplObjectStorage $nodes;

    /**
     * @var SplObjectStorage<Loader, Progress> the commit stage of every write, built where its write step stands
     */
    private SplObjectStorage $commits;

    /**
     * @var SplObjectStorage<Node\Read, Progress> the read stage of every source, built where its read stands
     */
    private SplObjectStorage $reads;

    /**
     * @var SplObjectStorage<Node, list<array{Node\Write, bool}>> the writes that read each spine node, with whether
     *                                                            the write is planned right after it
     */
    private SplObjectStorage $hosted;

    private ?SchemaNotDerivableException $refusal = null;

    public function __construct(
        private readonly ProgressStages $stages = new VoidProgress(),
    ) {
        /** @var SplObjectStorage<Node, PlannedNode> $nodes */
        $nodes = new SplObjectStorage();
        $this->nodes = $nodes;
        /** @var SplObjectStorage<Loader, Progress> $commits */
        $commits = new SplObjectStorage();
        $this->commits = $commits;
        /** @var SplObjectStorage<Node\Read, Progress> $reads */
        $reads = new SplObjectStorage();
        $this->reads = $reads;
        /** @var SplObjectStorage<Node, list<array{Node\Write, bool}>> $hosted */
        $hosted = new SplObjectStorage();
        $this->hosted = $hosted;
    }

    public function hosts(Node $host, Node\Write $write, bool $planHere): void
    {
        $this->hosted->offsetSet($host, [...$this->hostedBy($host), [$write, $planHere]]);
    }

    /**
     * @return list<array{Node\Write, bool}>
     */
    public function hostedBy(Node $host): array
    {
        return $this->hosted->offsetExists($host) ? $this->hosted->offsetGet($host) : [];
    }

    /**
     * Every stage is built in the order the plan is walked, which is the order a heartbeat lists them in.
     */
    public function planCommit(Loader $loader): void
    {
        if ($this->commits->offsetExists($loader)) {
            return;
        }

        // with no heartbeat, nothing is built for a stage: no root, no location
        $this->commits->offsetSet($loader, $this->stages->stage(
            ProgressStages::COMMIT,
            ProgressUnit::files,
            location: $loader instanceof FileLoader && !$this->stages instanceof VoidProgress
                ? FileCommit::locationOf($loader->destination())
                : null,
        ));
    }

    /**
     * @return SplObjectStorage<Loader, Progress>
     */
    public function commits(): SplObjectStorage
    {
        return $this->commits;
    }

    /**
     * The nodes of a frame joined into this plan are planned apart, their stages with this plan's.
     */
    public function joined(): self
    {
        $joined = new self($this->stages);
        $joined->commits = $this->commits;
        $joined->reads = $this->reads;

        return $joined;
    }

    public function planRead(Node\Read $read): void
    {
        $extractor = $read->extractor();
        $this->reads->offsetSet($read, $this->stages->stage(
            ProgressStages::READ,
            ProgressUnit::rows,
            location: $extractor instanceof FileExtractor && !$this->stages instanceof VoidProgress
                ? $extractor->source()->location()
                : null,
        ));
    }

    public function readStageOf(Node\Read $read): ?Progress
    {
        return $this->reads->offsetExists($read) ? $this->reads->offsetGet($read) : null;
    }

    public function add(Node $node, PlannedNode $planned): PlannedNode
    {
        $this->nodes[$node] = $planned;

        return $planned;
    }

    public function has(Node $node): bool
    {
        return $this->nodes->offsetExists($node);
    }

    /**
     * @throws InvalidLogicException when $node was never planned
     */
    public function of(Node $node): PlannedNode
    {
        return $this->nodes->offsetExists($node)
            ? $this->nodes[$node]
            : throw InvalidLogicException::because('Node %s was never planned', $node::class);
    }

    /**
     * Keeps the first refusal.
     */
    public function refuse(SchemaNotDerivableException $refusal): void
    {
        $this->refusal ??= $refusal;
    }

    /**
     * Non-null means the WHOLE plan runs raw - all or nothing.
     */
    public function refusal(): ?SchemaNotDerivableException
    {
        return $this->refusal;
    }

    /**
     * The steps a pipeline runs for $node: the bound ones, or the raw ones when any node of the plan refused a
     * schema - a refusal is plan-wide, so every node answers the same way.
     *
     * @throws InvalidLogicException when $node was never planned
     *
     * @return list<JoinStep|Loader|Processor|Transformer>
     */
    public function steps(Node $node): array
    {
        $planned = $this->of($node);

        return $this->refusal === null ? $planned->bound : $planned->steps;
    }
}
