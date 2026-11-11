<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Physical;

use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\Foundation\Progress;
use Flow\ETL\Foundation\ProgressStages;
use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageName;
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
     * @var SplObjectStorage<Loader|Transformer, Progress> the stage of every write and join_each, built where its step stands
     */
    private SplObjectStorage $stepStages;

    /**
     * @var SplObjectStorage<Loader, Progress> the write stage of every loader, built where its write stands
     */
    private SplObjectStorage $writeStages;

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
        /** @var SplObjectStorage<Loader|Transformer, Progress> $stepStages */
        $stepStages = new SplObjectStorage();
        $this->stepStages = $stepStages;
        /** @var SplObjectStorage<Loader, Progress> $writeStages */
        $writeStages = new SplObjectStorage();
        $this->writeStages = $writeStages;
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
    public function planWrite(Loader $loader): void
    {
        if ($this->writeStages->offsetExists($loader)) {
            return;
        }

        // with no heartbeat, nothing is built for a stage: no root, no location
        $subject = $loader instanceof FileLoader && !$this->stages instanceof VoidProgress
            ? FileCommit::locationOf($loader->destination())
            : null;
        $this->writeStages->offsetSet($loader, $this->stages->stage(StageName::write, subject: $subject));

        // only a write to files always reports; any other write gets its stage when it ends, listed once it reports
        if ($loader instanceof FileLoader) {
            $this->stepStages->offsetSet($loader, $this->stages->stage(
                StageName::commit,
                ProgressUnit::files,
                subject: $subject,
            ));
        }
    }

    /**
     * @return SplObjectStorage<Loader, Progress>
     */
    public function writes(): SplObjectStorage
    {
        return $this->writeStages;
    }

    public function planJoinEach(Transformer $step, string $subject): void
    {
        $this->stepStages->offsetSet($step, $this->stages->stage(StageName::join, subject: $subject));
    }

    /**
     * @return SplObjectStorage<Loader|Transformer, Progress>
     */
    public function stages(): SplObjectStorage
    {
        return $this->stepStages;
    }

    /**
     * The nodes of a frame joined into this plan are planned apart, their stages with this plan's.
     */
    public function joined(): self
    {
        $joined = new self($this->stages);
        $joined->stepStages = $this->stepStages;
        $joined->writeStages = $this->writeStages;
        $joined->reads = $this->reads;

        return $joined;
    }

    public function planRead(Node\Read $read): void
    {
        $extractor = $read->extractor();
        $listened = !$this->stages instanceof VoidProgress;
        $this->reads->offsetSet($read, $this->stages->stage(
            StageName::read,
            ProgressUnit::rows,
            subject: $extractor instanceof FileExtractor && $listened ? $extractor->source()->location() : null,
            total: $listened && !$read->criteria()->narrows() ? $extractor->statistics()->rows->exactly() : null,
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
