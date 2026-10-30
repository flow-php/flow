<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Processor;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Definitions;
use Generator;
use SensitiveParameter;

use function array_map;
use function array_values;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function is_array;

final readonly class ExpandingProcessor implements Processor
{
    /**
     * @param Definition<mixed>|string $column
     * @param BatchSize $batchSize the most rows one output batch holds
     */
    public function __construct(
        private string|Definition $column,
        public ScalarFunction $function,
        private RequiredColumns $carries,
        private BatchSize $batchSize = new BatchSize(1000),
        private ?BoundExpansion $bound = null,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        // @mago-ignore analysis:mixed-assignment
        $column = $data['column'] ?? null;
        $carried = type_array()->assert($data['carries'] ?? null);
        $names = array_values(array_map(type_string()->assert(...), type_array()->assert($carried['names'] ?? null)));
        $unbound = new self(
            is_array($column) ? Definitions::fromArray($column) : type_string()->assert($column),
            $rebuild->of($data['function'] ?? null, ScalarFunction::class),
            type_boolean()->assert($carried['all_but'] ?? null)
                ? RequiredColumns::allBut(...$names)
                : RequiredColumns::only(...$names),
            new BatchSize(type_integer()->assert($data['batch_size'] ?? null)),
        );

        return ($data['input'] ?? null) === null
            ? $unbound
            : type_instance_of(self::class)->assert($unbound->bind(Schema::fromArray(type_array()->assert(
                $data['input'],
            )))->step);
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'column' => $this->column instanceof Definition ? $this->column->normalize() : $this->column,
            'function' => $this->function,
            'carries' => ['all_but' => $this->carries->allBut, 'names' => $this->carries->names],
            'batch_size' => $this->batchSize->size,
            'input' => $this->bound?->input->normalize(),
        ]);
    }

    public function bind(Schema $input): BoundStep
    {
        $bound = BoundExpansion::of($this->column, $this->function, $this->carries, $input);

        return new BoundStep(
            new self($this->column, $this->function, $this->carries, $this->batchSize, $bound),
            $bound->output,
        );
    }

    public function breaks(): bool
    {
        return false;
    }

    /**
     * @param Generator<int, Rows> $rows
     *
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function process(Generator $rows, Backend $backend): Generator
    {
        while ($rows->valid()) {
            /** @var Rows $batch */
            $batch = $rows->current();
            // a plan that refused a schema runs this step unbound
            $bound = $this->bound ?? BoundExpansion::of(
                $this->column,
                $this->function,
                $this->carries,
                $batch->schema(),
            );

            foreach ($bound->chunks($batch, $backend, $this->batchSize->size) as $chunk) {
                $signal = yield $chunk;

                if ($signal === Signal::STOP) {
                    $rows->send(Signal::STOP);

                    return;
                }
            }

            $rows->next();
        }
    }
}
