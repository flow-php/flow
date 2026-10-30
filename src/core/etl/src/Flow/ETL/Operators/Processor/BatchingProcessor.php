<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Processor;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;
use SensitiveParameter;

use function Flow\Types\DSL\type_integer;

/**
 * Re-batches rows into fixed-size batches.
 */
final readonly class BatchingProcessor implements Processor
{
    public function __construct(
        public BatchSize $size,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(new BatchSize(type_integer()->assert($data['size'] ?? null)));
    }

    public function describe(): ?Description
    {
        return new Description(self::class, ['size' => $this->size->size]);
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
        $schema = null;

        /** @var list<Rows> $pending */
        $pending = [];
        $pendingCount = 0;

        while ($rows->valid()) {
            $batch = $rows->current();
            $schema ??= $batch->schema();

            if (!$batch->isEmpty()) {
                $pending[] = $batch->matchTo($schema, $backend);
                $pendingCount += $batch->count();

                while ($pendingCount >= $this->size->size) {
                    // one variadic concat per window copies each row once; a pairwise concat per child batch would
                    // re-copy everything pending
                    $window = Rows::concat($schema, $backend, ...$pending);
                    $signal = yield $window->slice(0, $this->size->size);

                    if ($signal === Signal::STOP) {
                        $rows->send(Signal::STOP);

                        return;
                    }

                    $pending = $window->count() > $this->size->size ? [$window->drop($this->size->size)] : [];
                    $pendingCount -= $this->size->size;
                }
            }

            $rows->next();
        }

        if ($schema !== null && $pendingCount > 0) {
            yield Rows::concat($schema, $backend, ...$pending);
        }
    }
}
