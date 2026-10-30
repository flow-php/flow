<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Processor;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\RandomValueGenerator;
use Flow\ETL\Kernels\Sort\ExternalSort;
use Flow\ETL\Kernels\Sort\ExternalSortConfig;
use Flow\ETL\Kernels\Sort\SortColumn;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Schema;
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
    ) {
        $this->sort = ExternalSort::of($columns, $config, $random);
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
                ])
                : null
        );
    }

    /**
     * null when it does not describe.
     */
    public function distribution(): ?Distribution
    {
        return $this->columns === [] || $this->describe() === null ? null : Distribution::ordered($this->columns);
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
        yield from $this->sort->sort($rows, $backend);
    }
}
