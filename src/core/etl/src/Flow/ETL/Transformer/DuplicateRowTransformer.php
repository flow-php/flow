<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\BoundStep;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ExpandingFunctions;
use Flow\ETL\Function\Parameter;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Transformer;
use Flow\ETL\WithEntry;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type\Unifier\NullabilityRule;
use Flow\Types\Type\Unifier\PromotingUnifier;
use Throwable;

use function array_values;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\Types\DSL\type_equals;

final class DuplicateRowTransformer implements Transformer
{
    /**
     * @var list<WithEntry>
     */
    private readonly array $entries;

    /**
     * The condition resolved against the bound schema, and the schema this step declares. Only
     * bind() sets them; the unbound path derives both per batch.
     */
    private mixed $resolved = null;

    private ?Schema $output = null;

    public function __construct(
        private readonly mixed $condition,
        WithEntry ...$entries,
    ) {
        $this->entries = array_values($entries);
    }

    public function bind(Schema $input): BoundStep
    {
        $bound = new self($this->condition, ...$this->entries);
        $bound->resolved = $this->resolve($input);
        $bound->output = $this->declare($input);

        return new BoundStep($bound, $bound->output);
    }

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $inputRowCount = $rows->count();

        $context->telemetry()->transformationStarted($this);

        try {
            // @mago-ignore analysis:mixed-assignment
            $condition = $this->resolved ?? $this->resolve($rows->schema());
            $output = $this->output ?? $this->declare($rows->schema());
            $duplicated = [];

            foreach ((new Parameter($condition))->asBooleans($rows, $context) as $position => $duplicate) {
                if ($duplicate ?? false) {
                    $duplicated[] = $position;
                }
            }

            if ($duplicated !== []) {
                // The maps inside ScalarFunctionTransformer are column-wise and stateless, so applying
                // each entry once over all duplicated rows is equivalent to applying it per row.
                $copies = $rows->gather($duplicated);

                foreach ($this->entries as $entry) {
                    $copies = (new ScalarFunctionTransformer($entry->name, $entry->function))->transform(
                        $copies,
                        $context,
                    );
                }

                $count = $rows->count();
                $copy = 0;
                $interleaved = [];

                // each copy follows the row it duplicates, so the output does not depend on where a batch ends
                for ($position = 0; $position < $count; $position++) {
                    $interleaved[] = $position;

                    if ($copy < count($duplicated) && $duplicated[$copy] === $position) {
                        $interleaved[] = $count + $copy++;
                    }
                }

                $rows = $rows
                    ->project($output, $context->backend())
                    ->concat($context->backend(), $copies->project($output, $context->backend()))
                    ->gather($interleaved);
            } else {
                $rows = $rows->project($output, $context->backend());
            }

            $context->telemetry()->transformationCompleted($this, [
                TelemetryAttributes::ATTR_TRANSFORMATION_INPUT_ROWS => $inputRowCount,
                TelemetryAttributes::ATTR_TRANSFORMATION_OUTPUT_ROWS => $rows->count(),
            ]);

            return $rows;
        } catch (Throwable $e) {
            $context->telemetry()->transformationFailed($this, $e);

            throw $e;
        }
    }

    /**
     * @throws InvalidTypeException
     */
    private function declare(Schema $input): Schema
    {
        $duplicated = $input;

        foreach ($this->entries as $entry) {
            (new ExpandingFunctions())->refuse($entry->function, 'duplicateRow');
            $duplicated = (new ScalarFunctionTransformer($entry->name, $entry->function))->bind($duplicated)->output;
        }

        $output = $input;

        foreach ($duplicated->definitions() as $name => $definition) {
            $existing = $output->findDefinition($name);

            if ($existing === null) {
                // the entries only apply to rows that matched the condition, so a column the
                // untouched half of the batch never carries has to be declared nullable
                $output = $output->add($definition->makeNullable());

                continue;
            }

            if (type_equals($existing->type(), $definition->type())) {
                continue;
            }

            $output = $output->replace($name, definition_from_type(
                $name,
                (new PromotingUnifier())->unifyAll(
                    NullabilityRule::ALL,
                    $existing->type(),
                    $definition->type(),
                ) ?? throw InvalidTypeException::noCommonType($existing->type(), $definition->type()),
            ));
        }

        return $output;
    }

    private function resolve(Schema $input): mixed
    {
        if (!$this->condition instanceof ScalarFunction) {
            return $this->condition;
        }

        $resolver = new ReferenceResolver();
        $resolved = $resolver->resolve($this->condition, $input);
        $resolver->assertResolved($resolved, $input);
        (new ExpandingFunctions())->refuse($resolved, 'duplicateRow');

        return $resolved;
    }
}
