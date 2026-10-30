<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Processor;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\ReferenceResolver;
use Flow\ETL\Functions\Window\BoundWindow;
use Flow\ETL\Functions\WindowFunction;
use Flow\ETL\Kernels\Sort\RowOrder;
use Flow\ETL\Kernels\Sort\SortColumn;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Definitions;
use Generator;
use SensitiveParameter;

use function array_map;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_string;
use function is_array;

/**
 * Applies window functions over partitioned and ordered data.
 */
final class WindowProcessor implements Processor
{
    /**
     * @param Definition<mixed>|string $column
     */
    public function __construct(
        public readonly string|Definition $column,
        public readonly WindowFunction $function,
        public readonly ?BoundWindow $bound = null,
    ) {}

    /**
     * @throws SchemaDefinitionNotFoundException
     */
    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        // @mago-ignore analysis:mixed-assignment
        $column = $data['column'] ?? null;
        $unbound = new self(
            is_array($column) ? Definitions::fromArray($column) : type_string()->assert($column),
            $rebuild->of($data['function'] ?? null, WindowFunction::class),
        );

        return ($data['input'] ?? null) === null
            ? $unbound
            : new self(
                $unbound->column,
                $unbound->function,
                $unbound->boundTo(Schema::fromArray(type_array()->assert($data['input']))),
            );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'column' => $this->column instanceof Definition ? $this->column->normalize() : $this->column,
            'function' => $this->function,
            'input' => $this->bound?->input->normalize(),
        ]);
    }

    public function bind(Schema $input): BoundStep
    {
        $bound = $this->boundTo($input);

        return new BoundStep(new self($this->column, $this->function, $bound), $bound->output);
    }

    public function breaks(): bool
    {
        return false;
    }

    public function process(Generator $rows, Backend $backend): Generator
    {
        $bound = $this->bound;

        foreach ($rows as $batch) {
            $bound ??= $this->boundTo($batch->schema());

            if (!$batch->count()) {
                yield Rows::empty($bound->output, $backend);

                continue;
            }

            // one incoming batch is one partition: the RepartitionProcessor before it put every row sharing the
            // partition key into a single Rows before this processor ever sees it
            yield $this->processPartition($bound, $batch, $backend);
        }
    }

    /**
     * @throws SchemaDefinitionNotFoundException
     */
    private function boundTo(Schema $input): BoundWindow
    {
        /** @var WindowFunction $resolved a window root is never a reference leaf */
        $resolved = (new ReferenceResolver())->bind($this->function, $input, 'over');
        self::assertWindowReferences($resolved, $input);

        $derived = $this->column instanceof Definition
            ? $this->column
            : Definitions::of($this->column, $resolved->returns());

        return new BoundWindow($resolved, $derived, $input, $input->put($derived));
    }

    private static function assertWindowReferences(WindowFunction $function, Schema $schema): void
    {
        $window = $function->window();

        foreach ([
            ...$window->partitions()->names(),
            ...array_map(static fn(SortColumn $column): string => $column->name, $window->order()),
        ] as $name) {
            if ($schema->findDefinition($name) === null) {
                throw SchemaDefinitionNotFoundException::withAvailable($name, ...$schema->names());
            }
        }
    }

    /**
     * @param Rows $partition never empty; process() answers a zero-row batch from the bound schema
     */
    private function processPartition(BoundWindow $bound, Rows $partition, Backend $backend): Rows
    {
        $resolved = $bound->resolved;
        $derived = $bound->derived;
        $window = $resolved->window();
        $orderBy = $window->order();
        $matched = $partition->matchTo($bound->input, $backend);
        $order = new RowOrder(
            $orderBy === []
                ? array_map(static fn(Reference $ref): SortColumn => $ref->asc(), $window->partitions()->all())
                : $orderBy,
        );
        $partitionRows = $matched->gather($order->permutation($order->keys($matched), $matched->count()));

        $builder = $backend->builder($derived);
        $builder->appendMany($resolved->values($partitionRows, $backend));

        return $partitionRows->withColumns($bound->output, [$derived->name() => $builder->finish()]);
    }
}
