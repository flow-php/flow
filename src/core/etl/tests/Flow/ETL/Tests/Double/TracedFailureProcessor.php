<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Schema;
use Generator;
use RuntimeException;
use SensitiveParameter;

/**
 * Fails on its first non-empty batch with a failure built inside the run, so its trace holds the run's frames.
 */
final class TracedFailureProcessor implements Processor
{
    public ?RuntimeException $thrown = null;

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

    public function process(Generator $rows, Backend $backend): Generator
    {
        foreach ($rows as $batch) {
            if ($batch->count() > 0) {
                throw $this->thrown = new RuntimeException('processor exploded');
            }

            yield $batch;
        }
    }
}
