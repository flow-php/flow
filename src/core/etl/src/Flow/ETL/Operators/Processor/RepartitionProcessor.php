<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Processor;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\RandomValueGenerator;
use Flow\ETL\Functions\Reference\References;
use Flow\ETL\Kernels\Bucketing\Buckets;
use Flow\ETL\Kernels\Bucketing\HashBucketing;
use Flow\ETL\Kernels\Bucketing\KeyEncoder;
use Flow\ETL\Kernels\Bucketing\KeyGrouping;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Storage\BucketingConfig;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Memory\BoundedRead;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;
use SensitiveParameter;

use function Flow\Types\DSL\type_instance_of;

/**
 * One batch per key. While the process stays under the memory limit the input is held and grouped in one pass; past
 * it, the input is partitioned into buckets and each bucket grouped on its own.
 */
final readonly class RepartitionProcessor implements Distributable
{
    public function __construct(
        public References $by,
        public BucketingConfig $config,
        private RandomValueGenerator $random,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['by'] ?? null, References::class),
            $rebuild->of($data['config'] ?? null, BucketingConfig::class),
            type_instance_of(RandomValueGenerator::class)->assert($rebuild->of(
                $data['random'] ?? null,
                Describable::class,
            )),
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
        [$read, $fits] = (new BoundedRead($this->config->memoryLimit, $backend))->read($rows);
        $buckets = new Buckets($this->config->spilling($backend));

        try {
            if ($fits) {
                yield from $grouping->group($this->concatenated($read, $backend));

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
                    yield $group;
                }
            }
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

        return $names === [] || $this->describe() === null ? null : Distribution::clustered($names);
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
