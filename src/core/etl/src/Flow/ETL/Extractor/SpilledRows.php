<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Schema\Inference\SchemaSampler;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Generator;
use Throwable;

use function base64_decode;
use function base64_encode;
use function bin2hex;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_string;
use function random_bytes;
use function serialize;
use function strlen;
use function unserialize;

/**
 * A source that can be read only once, made replayable: every row is written to a temporary
 * schema-less spill file as it streams past, and the replay reads the file back. The source is
 * advanced exactly once, ever; the spill it produced can be replayed as often as the caller likes,
 * and lives until this object is collected.
 */
final class SpilledRows implements SchemaSampler
{
    // Names no class: self::class would print a collaborator the caller never constructed and cannot
    // find in their own code, so the text describes the dataset, which is the thing they did pass.
    private const ALREADY_CONSUMED =
        'The dataset was already consumed: a source that cannot be read '
            . 'twice is spilled once and replayed once. Pass an array, declare the schema with '
            . '->withSchema(), or build a new extractor per run.';

    private readonly Path $path;

    private SpillState $state = SpillState::Fresh;

    /**
     * @param iterable<array<mixed>> $source read exactly once, ever
     * @param Path $spillRoot the extractor resolves it; this class owns the layout below, the way
     *                        FilesystemBuckets suffixes '/flow-php-buckets/' onto its own cacheDir
     * @param int<1, max> $bufferSize bytes held before a write; DestinationStream::append() costs one
     *                                write(2) per call, and one per row is 4x the cost of the whole fold
     */
    public function __construct(
        private readonly iterable $source,
        private readonly Filesystem $filesystem,
        Path $spillRoot,
        private readonly int $bufferSize = 65_536,
    ) {
        $this->path = $spillRoot->suffix('/flow-php-source/' . bin2hex(random_bytes(16)) . '.b64');
    }

    /**
     * The only place the spill is removed: it outlives every replay, so a generator source supports
     * repeated terminal operations the way an array one does.
     *
     * Swallows every Throwable: an exception escaping a destructor during shutdown is a PHP fatal, and
     * Filesystem::rm() is not exception-free.
     */
    public function __destruct()
    {
        if ($this->state === SpillState::Fresh || $this->state === SpillState::Deleted) {
            return;
        }

        try {
            $this->filesystem->rm($this->path);
        } catch (Throwable) {
        }

        $this->state = SpillState::Deleted;
    }

    public function path(): Path
    {
        return $this->path;
    }

    /**
     * Rows in the shape the source produced them, int keys still int - naming belongs to array_to_rows().
     *
     * Replayable as often as the caller likes - the file is removed by __destruct, not by the replay.
     *
     * A never-sampled sampler streams the source instead of a file; that arm IS one-shot, because there
     * is no file to read back.
     *
     * @return Generator<int, array<mixed>>
     *
     * @throws InvalidLogicException when the source was already replayed, or was abandoned mid-spill
     */
    public function rows(): Generator
    {
        if ($this->state === SpillState::Fresh) {
            $this->state = SpillState::Replaying;

            return (function (): Generator {
                foreach ($this->source as $row) {
                    yield $row;
                }

                $this->state = SpillState::Deleted;
            })();
        }

        if ($this->state !== SpillState::Spilled) {
            // Abandoned is the important arm: neither replaying an incomplete file nor re-streaming a
            // half-consumed source is correct, so both are refused rather than risked.
            throw InvalidLogicException::because(self::ALREADY_CONSUMED);
        }

        $this->state = SpillState::Replaying;

        return (function (): Generator {
            $source = $this->filesystem->readFrom($this->path);

            try {
                foreach ($source->readLines("\n") as $line) {
                    if ($line === '') {
                        continue;
                    }

                    yield type_array()->assert(unserialize(type_string()->assert(base64_decode($line, true)), [
                        'allowed_classes' => true,
                    ]));
                }
            } finally {
                // Back to Spilled, not Deleted: the file outlives the replay, so a second extract() -
                // or a schema() followed by a run() - replays it instead of being refused.
                $source->close();
                $this->state = SpillState::Spilled;
            }
        })();
    }

    /**
     * $rowBudget is IGNORED: every row is written regardless, which is what makes a partially written
     * spill unreachable by construction rather than merely unlikely.
     *
     * @return iterable<int, iterable<int, RawRowValues>>
     *
     * @throws InvalidLogicException when the source was already read
     */
    public function samples(int $rowBudget): iterable
    {
        if ($this->state !== SpillState::Fresh) {
            throw InvalidLogicException::because(self::ALREADY_CONSUMED);
        }

        $this->state = SpillState::Spilling;
        $stream = $this->filesystem->writeTo($this->path);

        // `return [...]`, never `yield`: a body containing `yield` makes samples() a generator, and
        // the guard, the Spilling transition and writeTo() above would all be deferred to the first
        // advance - so samples() twice without draining would be accepted.
        /** @var callable(): Generator<int, array<mixed>> $spilling */
        $spilling = function () use ($stream): Generator {
            $buffer = '';

            try {
                foreach ($this->source as $row) {
                    $buffer .= base64_encode(serialize($row)) . "\n";

                    if (strlen($buffer) >= $this->bufferSize) {
                        $stream->append($buffer);
                        $buffer = '';
                    }

                    yield $row;
                }

                if ($buffer !== '') {
                    $stream->append($buffer);
                }

                $this->state = SpillState::Spilled;
            } finally {
                // finally, not catch: PHP skips catch when a generator is destroyed, so a fold that
                // threw or stopped early - SchemaInferrer breaks out when its row budget runs out -
                // would otherwise leave the state Spilling and the stream open.
                if ($this->state === SpillState::Spilling) {
                    $this->state = SpillState::Abandoned;
                }

                $stream->close();
            }
        };

        // InMemoryRows owns the raw-row to RawRowValues naming; keeping a second copy here would put the
        // "same names array_to_rows() gives" invariant in two places.
        return (new InMemoryRows($spilling()))->samples($rowBudget);
    }

    public function state(): SpillState
    {
        return $this->state;
    }
}
