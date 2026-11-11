<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use ArrayObject;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Schema;
use Generator;
use SensitiveParameter;
use Throwable;

final readonly class RecordingProcessor implements Processor
{
    /**
     * @param ArrayObject<int, string> $log shared with the other steps of a run, so one list shows the order they end in
     */
    public function __construct(
        private ArrayObject $log,
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
        return new BoundStep($this, $input);
    }

    public function breaks(): bool
    {
        return false;
    }

    public function process(Generator $rows, Backend $backend): Generator
    {
        try {
            foreach ($rows as $batch) {
                yield $batch;
            }
        } catch (Throwable $failure) {
            $this->log->append('processor caught ' . $failure->getMessage());

            throw $failure;
        } finally {
            $this->log->append('processor finally');
        }
    }
}
