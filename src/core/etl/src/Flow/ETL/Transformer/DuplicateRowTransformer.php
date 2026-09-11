<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Parameter;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Pipeline\BoundStep;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Transformer;
use Flow\ETL\WithEntry;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type\Unifier\NullabilityRule;
use Flow\Types\Type\Unifier\PromotingUnifier;
use Throwable;

use function array_key_exists;
use function array_values;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\rows;
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
            $sources = [];

            foreach ($rows->all() as $position => $row) {
                if ((new Parameter($condition))->asBoolean($row, $context) ?? false) {
                    $duplicated[] = $row;
                    $sources[$position] = true;
                }
            }

            if ($duplicated !== []) {
                // The maps inside ScalarFunctionTransformer are per-row and stateless, so applying
                // each entry once over all duplicated rows is equivalent to applying it per row.
                $duplicatedRows = rows($rows->schema(), ...$duplicated);

                foreach ($this->entries as $entry) {
                    $duplicatedRows = (new ScalarFunctionTransformer($entry->name, $entry->function))->transform(
                        $duplicatedRows,
                        $context,
                    );
                }

                $copies = $duplicatedRows->all();
                $copy = 0;
                $interleaved = [];

                // each copy follows the row it duplicates, so the output does not depend on where a batch ends
                foreach ($rows->all() as $position => $row) {
                    $interleaved[] = $row;

                    if (array_key_exists($position, $sources)) {
                        $interleaved[] = $copies[$copy++];
                    }
                }

                $rows = new Rows($output, ...$interleaved);
            } else {
                $rows = new Rows($output, ...$rows->all());
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

        return $resolved;
    }
}
