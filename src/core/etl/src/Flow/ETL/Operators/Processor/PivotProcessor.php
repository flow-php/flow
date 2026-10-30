<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Processor;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Operators\GroupBy\PivotedTable;
use Flow\ETL\Operators\GroupBy\PivotShape;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Storage\BucketingConfig;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;
use SensitiveParameter;

use function Flow\Types\DSL\type_array;

final readonly class PivotProcessor implements Distributable
{
    /**
     * @param BucketingConfig $config the effective grouping config: the operation's own or the frame's
     */
    public function __construct(
        public GroupBy $groupBy,
        public BucketingConfig $config,
        private ?PivotShape $shape = null,
    ) {}

    /**
     * @throws InvalidArgumentException
     * @throws SchemaDefinitionNotFoundException
     */
    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        $groupBy = $rebuild->of($data['group_by'] ?? null, GroupBy::class);

        return new self(
            $groupBy,
            $rebuild->of($data['config'] ?? null, BucketingConfig::class),
            ($data['input'] ?? null) === null
                ? null
                : PivotShape::of($groupBy, Schema::fromArray(type_array()->assert($data['input']))),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'group_by' => $this->groupBy,
            'config' => $this->config,
            'input' => $this->shape?->input->normalize(),
        ]);
    }

    /**
     * The rows of one key meet in one process. null while unbound, for a global pivot, or when it does not describe.
     */
    public function distribution(): ?Distribution
    {
        $keys = $this->groupBy->refs()->names();

        return $this->shape === null || $keys === [] || $this->describe() === null
            ? null
            : Distribution::clustered($keys);
    }

    /**
     * The bound shape; null while unbound.
     */
    public function shape(): ?PivotShape
    {
        return $this->shape;
    }

    /**
     * @throws InvalidArgumentException when a pivot value collides with a group-by column
     * @throws SchemaDefinitionNotFoundException
     */
    public function bind(Schema $input): BoundStep
    {
        $shape = PivotShape::of($this->groupBy, $input);

        return new BoundStep(new self($this->groupBy, $this->config, $shape), $shape->output);
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
        if ($this->groupBy->aggregations()->count() === 0) {
            throw new RuntimeException('Pivot requires exactly one aggregation');
        }

        $table = $this->shape === null ? null : new PivotedTable($this->groupBy, $this->shape);

        foreach ($rows as $batch) {
            $table ??= new PivotedTable($this->groupBy, PivotShape::of($this->groupBy, $batch->schema()));
            $table->accumulate($batch, $backend);
        }

        if ($table !== null) {
            yield from $table->flush(new BatchSize($this->config->batchSize), $backend);
        }
    }
}
