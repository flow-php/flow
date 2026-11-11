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
use Flow\ETL\Kernels\Sort\ExternalSort;
use Flow\ETL\Kernels\Sort\ExternalSortConfig;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Memory\MemoryBudget;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Sort\SortColumn;
use Generator;
use SensitiveParameter;

use function array_values;
use function Flow\Types\DSL\type_instance_of;

final readonly class MergeSortProcessor implements Distributable
{
    public ExternalSort $sort;

    /**
     * @param ExternalSortConfig $config the effective sort config: the operation's own or the frame's
     */
    public function __construct(
        public ExternalSortConfig $config,
        /** @var list<SortColumn> */
        public array $columns,
        private RandomValueGenerator $random,
        private Progress $progress = new VoidProgress(),
    ) {
        $this->sort = ExternalSort::of($columns, $config, $random, $progress);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['config'] ?? null, ExternalSortConfig::class),
            array_values($rebuild->all($data['columns'] ?? null, SortColumn::class)),
            type_instance_of(RandomValueGenerator::class)->assert($rebuild->of(
                $data['random'] ?? null,
                Describable::class,
            )),
            $rebuild->progress(StageName::sort, $data['progress'] ?? null),
        );
    }

    /**
     * null when the random value generator does not describe.
     */
    public function describe(): ?Description
    {
        return (
            $this->random instanceof Describable
                ? Description::compose(self::class, [
                    'config' => $this->config,
                    'columns' => $this->columns,
                    'random' => $this->random,
                    'progress' => $this->progress instanceof Describable ? $this->progress : new VoidProgress(),
                ])
                : null
        );
    }

    /**
     * null when it does not describe.
     */
    public function distribution(): ?Distribution
    {
        return $this->columns === [] || $this->describe() === null
            ? null
            : Distribution::ordered($this->columns, $this->progress);
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input);
    }

    public function breaks(): bool
    {
        return true;
    }

    public function process(Generator $rows, Backend $backend): Generator
    {
        // pulled before the budget starts: what a lazy upstream allocates on its first pull (a join build) is not ours
        $rows->valid();

        yield from $this->sort->sort(
            $this->sort->counted($rows),
            $backend,
            MemoryBudget::of($backend, $this->config->bucketing->memoryLimit),
        );
        $this->progress->done();
    }
}
