<?php

declare(strict_types=1);

namespace Flow\ETL\Planner;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Plan\Node;
use Flow\ETL\Planner\Lowering\AggregateLowering;
use Flow\ETL\Planner\Lowering\BatchByLowering;
use Flow\ETL\Planner\Lowering\BatchLowering;
use Flow\ETL\Planner\Lowering\CacheLowering;
use Flow\ETL\Planner\Lowering\CollectLowering;
use Flow\ETL\Planner\Lowering\CollectRefsLowering;
use Flow\ETL\Planner\Lowering\ConstrainLowering;
use Flow\ETL\Planner\Lowering\CrossJoinLowering;
use Flow\ETL\Planner\Lowering\DiscardLowering;
use Flow\ETL\Planner\Lowering\DistinctLowering;
use Flow\ETL\Planner\Lowering\DropLowering;
use Flow\ETL\Planner\Lowering\DuplicateRowLowering;
use Flow\ETL\Planner\Lowering\FilterLowering;
use Flow\ETL\Planner\Lowering\JoinEachLowering;
use Flow\ETL\Planner\Lowering\JoinLowering;
use Flow\ETL\Planner\Lowering\LimitLowering;
use Flow\ETL\Planner\Lowering\OffsetLowering;
use Flow\ETL\Planner\Lowering\ReadLowering;
use Flow\ETL\Planner\Lowering\RenameEachLowering;
use Flow\ETL\Planner\Lowering\RenameLowering;
use Flow\ETL\Planner\Lowering\RepartitionLowering;
use Flow\ETL\Planner\Lowering\ResultLowering;
use Flow\ETL\Planner\Lowering\RowIndexLowering;
use Flow\ETL\Planner\Lowering\SelectLowering;
use Flow\ETL\Planner\Lowering\SinkMultipleLowering;
use Flow\ETL\Planner\Lowering\SortLowering;
use Flow\ETL\Planner\Lowering\TransactionLowering;
use Flow\ETL\Planner\Lowering\TransformLowering;
use Flow\ETL\Planner\Lowering\UntilLowering;
use Flow\ETL\Planner\Lowering\ValidateLowering;
use Flow\ETL\Planner\Lowering\WindowColumnLowering;
use Flow\ETL\Planner\Lowering\WithColumnLowering;
use Flow\ETL\Planner\Lowering\WriteLowering;

final readonly class Lowerings
{
    /**
     * @var array<class-string<Node>, Lowering<Node>>
     */
    private array $byKind;

    /**
     * @param Lowering<Node> ...$lowerings
     */
    public function __construct(Lowering ...$lowerings)
    {
        $byKind = [];

        foreach ($lowerings as $lowering) {
            $byKind[$lowering->handles()] = $lowering;
        }

        $this->byKind = $byKind;
    }

    public static function default(): self
    {
        return new self(
            new ReadLowering(),
            new AggregateLowering(),
            new BatchLowering(),
            new BatchByLowering(),
            new CacheLowering(),
            new CollectLowering(),
            new CollectRefsLowering(),
            new ConstrainLowering(),
            new CrossJoinLowering(),
            new DistinctLowering(),
            new DropLowering(),
            new DiscardLowering(),
            new DuplicateRowLowering(),
            new FilterLowering(),
            new JoinEachLowering(),
            new JoinLowering(),
            new LimitLowering(),
            new OffsetLowering(),
            new RenameLowering(),
            new RenameEachLowering(),
            new RepartitionLowering(),
            new ResultLowering(),
            new RowIndexLowering(),
            new SelectLowering(),
            new SinkMultipleLowering(),
            new SortLowering(),
            new TransactionLowering(),
            new TransformLowering(),
            new UntilLowering(),
            new ValidateLowering(),
            new WindowColumnLowering(),
            new WithColumnLowering(),
            new WriteLowering(),
        );
    }

    /**
     * @return Lowering<Node>
     *
     * @throws InvalidLogicException a node kind with no lowering is a programming error
     */
    public function of(Node $node): Lowering
    {
        return $this->byKind[$node::class] ?? throw InvalidLogicException::nodeNotLowerable($node::class);
    }
}
