<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ExpandingFunctions;
use Flow\ETL\Function\FrameAccumulating;
use Flow\ETL\Function\PartitionRanking;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\WindowFunction;
use Flow\ETL\Pipeline\BoundStep;
use Flow\ETL\Processor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Window\BoundWindow;
use Flow\ETL\Window\WindowContext;
use Flow\ETL\Window\WindowFrame;
use Generator;

use function array_values;
use function count;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\rows;

/**
 * Applies window functions over partitioned and ordered data.
 *
 * @internal
 */
final class WindowProcessor implements Processor
{
    /**
     * @param Definition<mixed>|string $entry
     */
    public function __construct(
        private readonly string|Definition $entry,
        private readonly WindowFunction $function,
        private readonly ?BoundWindow $bound = null,
    ) {}

    public function bind(Schema $input): BoundStep
    {
        $bound = $this->boundTo($input);

        return new BoundStep(new self($this->entry, $this->function, $bound), $bound->output);
    }

    public function process(Generator $rows, FlowContext $context): Generator
    {
        $bound = $this->bound;

        foreach ($rows as $batch) {
            $bound ??= $this->boundTo($batch->schema());

            if (!$batch->count()) {
                yield new Rows($bound->output);

                continue;
            }

            // one incoming batch is one partition: RepartitionSteps put every row sharing the
            // partition key into a single Rows before this processor ever sees it
            yield $this->processPartition($bound, $batch->all(), $context);
        }
    }

    /**
     * @throws SchemaDefinitionNotFoundException
     */
    private function boundTo(Schema $input): BoundWindow
    {
        $resolver = new ReferenceResolver();
        /** @var WindowFunction $resolved a window root is never a reference leaf */
        $resolved = $resolver->resolve($this->function, $input);
        $resolver->assertResolved($resolved, $input);
        (new ExpandingFunctions())->refuse($resolved, 'over');
        self::assertWindowReferences($resolved, $input);

        $derived = $this->entry instanceof Definition
            ? $this->entry
            : definition_from_type($this->entry, $resolved->returns());

        $name = $derived->entry()->name();

        return new BoundWindow(
            $resolved,
            $derived,
            $input,
            $input->findDefinition($name) === null ? $input->add($derived) : $input->replace($name, $derived),
        );
    }

    /**
     * An order reference already failed loudly - sortBy() reaches Rows::sortDescending(), which throws
     * on the first row of the first partition. Only the partition path degraded silently, because
     * extractPartitionKey() substitutes null for a missing entry. This check makes both refuse
     * identically, one batch earlier, with the gate's message.
     */
    private static function assertWindowReferences(WindowFunction $function, Schema $schema): void
    {
        $window = $function->window();

        foreach ([...$window->partitions()->all(), ...$window->order()] as $ref) {
            if ($schema->findDefinition($ref->to()) === null) {
                throw SchemaDefinitionNotFoundException::withAvailable($ref->to(), ...$schema->references()->names());
            }
        }
    }

    /**
     * Evaluates a frame-only window function for every row of the partition, reusing the previous
     * result whenever the frame bounds did not move.
     *
     * @return array<int, mixed>
     */
    private function accumulateValues(
        FrameAccumulating $function,
        WindowFrame $frame,
        Rows $partition,
        FlowContext $context,
    ): array {
        // Rows guarantees a list internally, but all() is typed array<Row> - narrow it here rather than
        // widen the shared Rows contract. One copy per partition, not per row.
        $rows = array_values($partition->all());
        $count = count($rows);
        $values = [];
        $accumulator = null;
        $previousBounds = null;
        /** @var mixed $value */
        $value = null;

        for ($index = 0; $index < $count; $index++) {
            $bounds = $frame->bounds($index, $partition);

            if ($bounds === $previousBounds) {
                $values[$index] = $value;

                continue;
            }

            // Equal starts with a grown end mean the previous frame is a prefix of this one, so the
            // rows already accumulated still belong to it and only the new tail has to be fed.
            if (
                $accumulator !== null
                && $previousBounds !== null
                && $bounds[0] === $previousBounds[0]
                && $bounds[1] > $previousBounds[1]
            ) {
                for ($i = $previousBounds[1] + 1; $i <= $bounds[1]; $i++) {
                    $accumulator->accumulate($rows[$i]);
                }
            } else {
                $accumulator = $function->accumulator($context);

                for ($i = $bounds[0]; $i <= $bounds[1]; $i++) {
                    $accumulator->accumulate($rows[$i]);
                }
            }

            // @mago-ignore analysis:mixed-assignment
            $value = $accumulator->value();
            $previousBounds = $bounds;
            $values[$index] = $value;
        }

        return $values;
    }

    /**
     * @param array<Row> $rows - never empty; process() answers a zero-row batch from the bound schema
     */
    private function processPartition(BoundWindow $bound, array $rows, FlowContext $context): Rows
    {
        $resolved = $bound->resolved;
        $derived = $bound->derived;
        $window = $resolved->window();
        $orderBy = $window->order();
        $sortBy = $orderBy === [] ? $window->partitions()->all() : $orderBy;
        $partitionRows = rows($bound->input, ...$rows)->sortBy(...$sortBy);

        $frame = $window->frame();
        $processedRows = [];

        $values = match (true) {
            $resolved instanceof PartitionRanking => $resolved->rankPartition($partitionRows),
            $resolved instanceof FrameAccumulating => $this->accumulateValues(
                $resolved,
                $frame,
                $partitionRows,
                $context,
            ),
            default => null,
        };

        foreach ($partitionRows as $index => $row) {
            $value = $values === null
                ? $resolved->apply(new WindowContext($row, $index, $partitionRows, $frame, $context))
                : $values[$index];

            $processedRows[] = new Row([
                ...$row->values(),
                $derived->entry()->name() => $value === null ? null : $derived->type()->cast($value),
            ]);
        }

        return rows($bound->output, ...$processedRows);
    }
}
