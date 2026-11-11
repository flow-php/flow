<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Processor;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Progress;
use Flow\ETL\Foundation\RandomValueGenerator;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Foundation\VoidProgress;
use Flow\ETL\Functions\Reference\References;
use Flow\ETL\Kernels\Bucketing\Buckets;
use Flow\ETL\Kernels\Bucketing\HashBucketing;
use Flow\ETL\Kernels\Bucketing\KeyGrouping;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Storage\BucketingConfig;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Grouping\KeyEncoder;
use Flow\ETL\Values\Memory\BoundedRead;
use Flow\ETL\Values\Memory\MemoryBudget;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;
use SensitiveParameter;

use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_string;

/**
 * One batch per key; past the memory limit, input is bucketed and grouped per bucket.
 */
final readonly class RepartitionProcessor implements Distributable
{
    public function __construct(
        public References $by,
        public BucketingConfig $config,
        private RandomValueGenerator $random,
        private Progress $progress = new VoidProgress(),
        private StageName $stage = StageName::repartition,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        $stage = StageName::from(type_string()->assert($data['stage'] ?? StageName::repartition->value));

        return new self(
            $rebuild->of($data['by'] ?? null, References::class),
            $rebuild->of($data['config'] ?? null, BucketingConfig::class),
            type_instance_of(RandomValueGenerator::class)->assert($rebuild->of(
                $data['random'] ?? null,
                Describable::class,
            )),
            $rebuild->progress($stage, $data['progress'] ?? null),
            $stage,
        );
    }

    /**
     * null when the random value generator or the bucketing config does not describe.
     */
    public function describe(): ?Description
    {
        return (
            $this->random instanceof Describable
                ? Description::compose(self::class, [
                    'by' => $this->by,
                    'config' => $this->config,
                    'random' => $this->random,
                    'stage' => $this->stage->value,
                    'progress' => $this->progress instanceof Describable ? $this->progress : new VoidProgress(),
                ])
                : null
        );
    }

    public function breaks(): bool
    {
        return true;
    }

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<Rows>
     */
    public function process(Generator $rows, Backend $backend): Generator
    {
        $grouping = new KeyGrouping(new KeyEncoder($this->by->names()), $backend);
        // pulled before the budget starts: what a lazy upstream allocates on its first pull (a join build) is not ours
        $rows->valid();
        $budget = MemoryBudget::of($backend, $this->config->memoryLimit);
        $buckets = new Buckets($this->config->spilling($backend, $budget));
        [$read, $fits] = (new BoundedRead($budget))->read($rows);

        try {
            if ($fits) {
                foreach ($grouping->group($this->concatenated($read, $backend)) as $group) {
                    $this->progress->advanced($group->count());

                    yield $group;
                }

                $this->progress->done();

                return;
            }

            foreach ((new HashBucketing(
                new KeyEncoder($this->by->names()),
                $this->config->bucketsCount,
                $this->random,
                'repartition',
            ))->bucketize(BoundedRead::followedBy($read, $rows), $buckets->storage()) as $bucket) {
                $buckets->add($bucket);
            }

            foreach ($buckets->all() as $bucket) {
                // re-key batches, yield from would restart keys at 0 for every bucket
                foreach ($grouping->group($buckets->rows($bucket->id)) as $group) {
                    $this->progress->advanced($group->count());

                    yield $group;
                }
            }

            $this->progress->done();
        } finally {
            $buckets->clear();
        }
    }

    /**
     * null when it does not describe.
     */
    public function distribution(): ?Distribution
    {
        $names = $this->by->names();

        return $names === [] || $this->describe() === null
            ? null
            : Distribution::clustered($names, progress: $this->progress);
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input);
    }

    /**
     * The held batches as one, so each key is gathered once rather than once per batch.
     *
     * @param list<Rows> $batches
     *
     * @return Generator<Rows>
     */
    public function concatenated(array $batches, Backend $backend): Generator
    {
        $parts = [];
        $schema = null;

        foreach ($batches as $batch) {
            if ($batch->isEmpty()) {
                continue;
            }

            $schema ??= $batch->schema();
            $parts[] = $batch->matchTo($schema, $backend);
        }

        if ($parts !== []) {
            yield Rows::concat($parts[0]->schema(), $backend, ...$parts);
        }
    }
}
