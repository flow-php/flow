<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Processor;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\ConstraintViolationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Constraint;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;
use SensitiveParameter;

/**
 * Validates constraints on each row.
 */
final class ConstrainedProcessor implements Processor
{
    private int $rowIndex = 0;

    /**
     * @param array<Constraint> $constraints
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        public readonly array $constraints = [],
    ) {
        foreach ($constraints as $constraint) {
            // @mago-ignore analysis:impossible-condition
            if (!$constraint instanceof Constraint) {
                throw new InvalidArgumentException(
                    'Pipeline constraints must be of type Flow\ETL\Operators\Constraint',
                );
            }
        }
    }

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
        return new BoundStep($this, $input);
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

            $violationIndex = null;
            $violated = null;

            // a constraint sees only the rows before the earliest violation found so far - the row-by-row order
            // in which an earlier constraint's violation stops the later ones
            foreach ($this->constraints as $constraint) {
                $index = $constraint->firstViolation(
                    $violationIndex === null ? $batch : $batch->slice(0, $violationIndex),
                );

                if ($index !== null) {
                    $violationIndex = $index;
                    $violated = $constraint;
                }
            }

            if ($violated !== null && $violationIndex !== null) {
                throw new ConstraintViolationException(
                    $violated->toString(),
                    $violated->violation($batch, $violationIndex),
                    $this->rowIndex + $violationIndex,
                );
            }

            $this->rowIndex += $batch->count();

            $signal = yield $batch;

            if ($signal === Signal::STOP) {
                $rows->send(Signal::STOP);

                return;
            }

            $rows->next();
        }
    }
}
