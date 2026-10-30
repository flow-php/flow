<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Transformer;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\ExpandingFunctions;
use Flow\ETL\Functions\Parameter;
use Flow\ETL\Functions\ReferenceResolver;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Functions\WithColumn;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definitions;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type\Unifier\NullabilityRule;
use Flow\Types\Type\Unifier\PromotingUnifier;
use SensitiveParameter;

use function array_key_last;
use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_instance_of;

final readonly class DuplicateRowTransformer implements Transformer
{
    /**
     * @param list<WithColumn> $columns
     * @param mixed $resolved the condition resolved against the bound schema; null while unbound
     * @param null|Schema $output the schema this step declares; null while unbound, the unbound path derives it per batch
     * @param null|list<BoundStep> $steps the column steps, each bound over the previous one's output; null while unbound
     */
    public function __construct(
        private mixed $condition,
        private array $columns,
        private mixed $resolved = null,
        private ?Schema $output = null,
        private ?array $steps = null,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }

    public function bind(Schema $input): BoundStep
    {
        $steps = $this->steps($input);
        $output = $this->declare($input, $steps);

        return new BoundStep(
            new self($this->condition, $this->columns, $this->resolve($input), $output, $steps),
            $output,
        );
    }

    public function transform(Rows $rows, Backend $backend): Rows
    {
        // @mago-ignore analysis:mixed-assignment
        $condition = $this->resolved ?? $this->resolve($rows->schema());
        $steps = $this->steps ?? $this->steps($rows->schema());
        $output = $this->output ?? $this->declare($rows->schema(), $steps);
        $duplicated = [];

        foreach ((new Parameter($condition))->asBooleans($rows, $backend) as $position => $duplicate) {
            if ($duplicate ?? false) {
                $duplicated[] = $position;
            }
        }

        if ($duplicated !== []) {
            // The maps inside ScalarFunctionTransformer are column-wise and stateless, so applying
            // each column once over all duplicated rows is equivalent to applying it per row.
            $copies = $rows->gather($duplicated);

            foreach ($steps as $step) {
                $copies = type_instance_of(Transformer::class)->assert($step->step)->transform($copies, $backend);
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

            return Rows::concat(
                $output,
                $backend,
                $rows->project($output, $backend),
                $copies->project($output, $backend),
            )->gather($interleaved);
        }

        return $rows->project($output, $backend);
    }

    /**
     * @return list<BoundStep>
     */
    private function steps(Schema $input): array
    {
        $steps = [];
        $schema = $input;

        foreach ($this->columns as $column) {
            (new ExpandingFunctions())->refuse($column->function, 'duplicateRow');
            $step = (new ScalarFunctionTransformer($column->name, $column->function))->bind($schema);
            $steps[] = $step;
            $schema = $step->output;
        }

        return $steps;
    }

    /**
     * @param list<BoundStep> $steps
     *
     * @throws InvalidTypeException
     */
    private function declare(Schema $input, array $steps): Schema
    {
        $duplicated = $steps === [] ? $input : $steps[array_key_last($steps)]->output;
        $output = $input;

        foreach ($duplicated->definitions() as $name => $definition) {
            $existing = $output->findDefinition($name);

            if ($existing === null) {
                // the columns only apply to rows that matched the condition, so a column the
                // untouched half of the batch never carries has to be declared nullable
                $output = $output->add($definition->makeNullable());

                continue;
            }

            if (type_equals($existing->type(), $definition->type())) {
                continue;
            }

            $output = $output->replace($name, Definitions::of(
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

        return (new ReferenceResolver())->bind($this->condition, $input, 'duplicateRow');
    }
}
