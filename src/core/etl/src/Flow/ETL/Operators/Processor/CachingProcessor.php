<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Processor;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Storage\RowsStorage;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;
use SensitiveParameter;
use Throwable;

/**
 * An existing cache id passes data through unchanged.
 */
final readonly class CachingProcessor implements Processor
{
    /**
     * @param bool $idGiven false when the id is the run config's: explain prints only a given id, so it stays reproducible
     */
    public function __construct(
        public string $id,
        public RowsStorage $cache,
        public bool $idGiven = true,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
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
        if ($this->cache->has($this->id)) {
            yield from $rows;

            return;
        }

        // a write cut short by a killed process leaves a key has() does not count; it is written again
        $this->cache->remove($this->id);
        $stopped = false;
        $empty = true;

        try {
            // a stop still reads the rest: a cached prefix would serve truncated data on the next read
            foreach ($rows as $batch) {
                $this->cache->append($this->id, $batch);
                $empty = false;

                if (!$stopped) {
                    $signal = yield $batch;
                    $stopped = $signal === Signal::STOP;
                }
            }

            // an empty stream is cached too, so the next read does not run the source again
            if ($empty) {
                $this->cache->append($this->id, Rows::empty(new Schema(), $backend));
            }

            $this->cache->close($this->id);
        } catch (Throwable $exception) {
            $this->cache->remove($this->id);

            throw $exception;
        }
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input);
    }
}
