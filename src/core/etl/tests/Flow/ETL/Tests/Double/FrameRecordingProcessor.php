<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;
use RuntimeException;
use SensitiveParameter;

use function array_key_exists;
use function debug_backtrace;
use function Flow\Types\DSL\type_array;

final class FrameRecordingProcessor implements Processor
{
    public ?bool $failedUnderUserCode = null;

    public bool $heldEnded = false;

    public function __construct(
        private readonly FailsWhen $when,
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

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<int, Rows, null|Signal, void>
     */
    public function process(Generator $rows, Backend $backend): Generator
    {
        // the only reference to a generator suspended in try/finally: its close is what reads the resuming frame
        $held = (function (): Generator {
            try {
                yield 1;
                yield 2;
            } finally {
                $this->heldEnded = true;
            }
        })();
        $held->current();
        $batches = 0;

        foreach ($rows as $batch) {
            if ($this->when === FailsWhen::secondBatch && ++$batches === 2) {
                // a frame without a file is an internal call: the resumer PHP can crash under
                $this->failedUnderUserCode = array_key_exists(
                    'file',
                    type_array()->assert(debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS)[0]),
                );

                throw new RuntimeException('processor failed');
            }

            $signal = yield $batch;

            if ($signal === Signal::STOP && $this->when === FailsWhen::stop) {
                $this->failedUnderUserCode = array_key_exists(
                    'file',
                    type_array()->assert(debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS)[0]),
                );

                throw new RuntimeException('processor failed on stop');
            }
        }
    }
}
