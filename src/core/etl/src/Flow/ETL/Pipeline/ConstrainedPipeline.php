<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Exception\{ConstraintViolationException, InvalidArgumentException};
use Flow\ETL\{Constraint, Extractor, FlowContext, Loader, Pipeline, Transformer};

final class ConstrainedPipeline implements Pipeline
{
    private int $rowIndex = 0;

    /**
     * @param Pipeline $pipeline
     * @param array<Constraint> $constraints
     *
     * @throws InvalidArgumentException
     */
    public function __construct(private readonly Pipeline $pipeline, private readonly array $constraints = [])
    {
        foreach ($constraints as $constraint) {
            if (!$constraint instanceof Constraint) {
                throw new InvalidArgumentException('Pipeline constraints must be of type Flow\ETL\Constraint');
            }
        }
    }

    public function add(Loader|Transformer $pipe) : self
    {
        $this->pipeline->add($pipe);

        return $this;
    }

    public function has(string $transformerClass) : bool
    {
        return $this->pipeline->has($transformerClass);
    }

    public function pipes() : Pipes
    {
        return $this->pipeline->pipes();
    }

    public function process(FlowContext $context) : \Generator
    {
        foreach ($this->pipeline->process($context) as $rows) {
            foreach ($rows->all() as $row) {
                foreach ($this->constraints as $constraint) {
                    if (!$constraint->isSatisfiedBy($row)) {
                        throw new ConstraintViolationException(
                            $constraint->toString(),
                            $constraint->violation($row),
                            $this->rowIndex
                        );
                    }
                }

                $this->rowIndex++;
            }

            yield $rows;
        }
    }

    public function source() : Extractor
    {
        return $this->pipeline->source();
    }
}
