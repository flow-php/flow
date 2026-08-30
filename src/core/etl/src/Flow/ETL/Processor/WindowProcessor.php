<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\FrameAccumulating;
use Flow\ETL\Function\PartitionRanking;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\WindowFunction;
use Flow\ETL\Processor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Window\WindowContext;
use Flow\ETL\Window\WindowFrame;
use Generator;

use function array_values;
use function count;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\rows;
use function serialize;

/**
 * Applies window functions over partitioned and ordered data.
 *
 * @internal
 */
final readonly class WindowProcessor implements Processor
{
    /**
     * @param Definition<mixed>|string $entry
     */
    public function __construct(
        private string|Definition $entry,
        private WindowFunction $function,
    ) {}

    public function process(Generator $rows, FlowContext $context): Generator
    {
        $currentPartitionKey = null;
        /** @var array<Row> $partitionRows */
        $partitionRows = [];
        $bound = null;
        $definition = null;
        $schema = null;
        $outputSchema = null;

        foreach ($rows as $batch) {
            // Bind once per run, against the first non-empty batch's schema - one Definition for the
            // whole produced column, before any row is read.
            if ($bound === null || $definition === null) {
                if (!$batch->count()) {
                    continue;
                }

                $schema = $batch->schema();
                $resolver = new ReferenceResolver();
                /** @var WindowFunction $bound a window root is never a reference leaf */
                $bound = $resolver->resolve($this->function, $schema);
                $resolver->assertResolved($bound, $schema);
                $this->assertWindowReferences($bound, $schema);

                $definition = $this->entry instanceof Definition
                    ? $this->entry
                    : definition_from_type($this->entry, $bound->returns());
                $outputSchema = $schema->findDefinition($definition->entry()->name()) === null
                    ? $schema->add($definition)
                    : $schema->replace($definition->entry()->name(), $definition);
            }

            foreach ($batch as $row) {
                $partitionKey = $this->extractPartitionKey($bound, $row);

                if (
                    $currentPartitionKey !== null
                    && $currentPartitionKey !== $partitionKey
                    && $schema !== null
                    && $outputSchema !== null
                ) {
                    yield $this->processPartition(
                        $bound,
                        $definition,
                        $schema,
                        $outputSchema,
                        $partitionRows,
                        $context,
                    );

                    $partitionRows = [];
                }

                $partitionRows[] = $row;
                $currentPartitionKey = $partitionKey;
            }
        }

        if (
            [] !== $partitionRows
            && $bound !== null
            && $definition !== null
            && $schema !== null
            && $outputSchema !== null
        ) {
            yield $this->processPartition($bound, $definition, $schema, $outputSchema, $partitionRows, $context);
        }
    }

    /**
     * An order reference already failed loudly - sortBy() reaches Rows::sortDescending(), which throws
     * on the first row of the first partition. Only the partition path degraded silently, because
     * extractPartitionKey() substitutes null for a missing entry. This check makes both refuse
     * identically, one batch earlier, with the gate's message.
     */
    private function assertWindowReferences(WindowFunction $function, Schema $schema): void
    {
        $window = $function->window();

        foreach ([...$window->partitions(), ...$window->order()] as $ref) {
            if ($schema->findDefinition($ref->to()) === null) {
                throw SchemaDefinitionNotFoundException::withAvailable($ref->to(), ...$schema->references()->names());
            }
        }
    }

    private function extractPartitionKey(WindowFunction $function, Row $row): string
    {
        $partitions = $function->window()->partitions();

        if ([] === $partitions) {
            return '__single_partition__';
        }

        $keyParts = [];

        foreach ($partitions as $partition) {
            try {
                $keyParts[] = $row->get($partition);
            } catch (InvalidArgumentException) {
                $keyParts[] = null;
            }
        }

        return serialize($keyParts);
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
     * Both call sites guarantee a non-empty partition - one is guarded by `[] !== $partitionRows`, the
     * other by a non-null current partition key, which is only set after a row has been appended.
     *
     * @param Definition<mixed> $definition
     * @param array<Row> $rows
     */
    private function processPartition(
        WindowFunction $function,
        Definition $definition,
        Schema $inputSchema,
        Schema $outputSchema,
        array $rows,
        FlowContext $context,
    ): Rows {
        $window = $function->window();
        $orderBy = $window->order();
        $partitionRows = rows($inputSchema, ...$rows)->sortBy(...$orderBy ?: $window->partitions());

        $frame = $window->frame();
        $processedRows = [];

        $values = match (true) {
            $function instanceof PartitionRanking => $function->rankPartition($partitionRows),
            $function instanceof FrameAccumulating => $this->accumulateValues(
                $function,
                $frame,
                $partitionRows,
                $context,
            ),
            default => null,
        };

        foreach ($partitionRows as $index => $row) {
            $value = $values === null
                ? $function->apply(new WindowContext($row, $index, $partitionRows, $frame, $context))
                : $values[$index];

            $processedRows[] = new Row([
                ...$row->values(),
                $definition->entry()->name() => $value === null ? null : $definition->type()->cast($value),
            ]);
        }

        return rows($outputSchema, ...$processedRows);
    }
}
