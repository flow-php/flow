<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Column\Backend;
use Flow\ETL\Column\Layout\BufferLayout;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\Exception\FloeException;

use function array_sum;
use function array_values;
use function intdiv;
use function sprintf;
use function strlen;
use function substr;
use function unpack;

final readonly class FrameDecoder
{
    public function __construct(
        private Codec $codec = new NoopCodec(),
        private BufferLayout $layout = new BufferLayout(),
    ) {}

    /**
     * @throws FloeException
     */
    public function decode(string $body, Schema $schema, Backend $backend): Rows
    {
        if (strlen($body) < 12) {
            throw new FloeException('Floe BATCH frame is truncated, its directory is incomplete');
        }

        /** @var array{1: int, 2: int, 3: int} $counts */
        $counts = unpack('V3', $body);
        [1 => $rowCount, 2 => $nodeCount, 3 => $bufferCount] = $counts;
        $nodeCounts = [];
        $bufferCounts = [];

        foreach ($schema->definitions() as $name => $definition) {
            $nodeCounts[$name] = $this->layout->nodeCount($definition->type());
            $bufferCounts[$name] = $this->layout->bufferCount($definition->type());
        }

        $expectedNodes = array_sum($nodeCounts);
        $expectedBuffers = array_sum($bufferCounts);

        if ($nodeCount !== $expectedNodes) {
            throw new FloeException(sprintf(
                'Floe BATCH frame holds %d nodes, the schema describes %d',
                $nodeCount,
                $expectedNodes,
            ));
        }

        if ($bufferCount !== $expectedBuffers) {
            throw new FloeException(sprintf(
                'Floe BATCH frame holds %d buffers, the schema describes %d',
                $bufferCount,
                $expectedBuffers,
            ));
        }

        $areaStart = intdiv(12 + (8 * ($nodeCount + $bufferCount)) + 7, 8) * 8;

        if (strlen($body) < $areaStart) {
            throw new FloeException('Floe BATCH frame is truncated, its directory is incomplete');
        }

        /** @var list<int> $words */
        $words = ($nodeCount + $bufferCount) === 0
            ? []
            : array_values((array) unpack('V' . (2 * ($nodeCount + $bufferCount)), $body, 12));
        $area = substr($body, $areaStart);
        $areaLength = strlen($area);
        $node = 0;
        $extent = 2 * $nodeCount;
        $buffer = 0;
        $columns = [];

        foreach ($schema->definitions() as $name => $definition) {
            $type = $definition->type();
            $length = $words[2 * $node];
            $nullCount = $words[(2 * $node) + 1];

            if ($length !== $rowCount) {
                throw new FloeException(sprintf(
                    'Floe BATCH frame column "%s" holds %d rows, the frame %d',
                    $name,
                    $length,
                    $rowCount,
                ));
            }

            if ($nullCount > $length) {
                throw new FloeException(sprintf(
                    'Floe BATCH frame column "%s" has %d nulls in %d rows',
                    $name,
                    $nullCount,
                    $length,
                ));
            }

            $buffers = [];

            for ($i = $bufferCounts[$name]; $i > 0; $i--, $buffer++, $extent += 2) {
                $offset = $words[$extent];
                $stored = $words[$extent + 1];

                if ($stored === 0) {
                    $buffers[] = '';

                    continue;
                }

                if (($offset + $stored) > $areaLength) {
                    throw new FloeException(sprintf(
                        'Floe BATCH frame buffer %d lies outside the %d-byte buffer area',
                        $buffer,
                        $areaLength,
                    ));
                }

                if ($stored < 8) {
                    throw new FloeException(sprintf(
                        'Floe BATCH frame buffer %d is shorter than its 8-byte length prefix',
                        $buffer,
                    ));
                }

                /** @var array{1: int} $prefix */
                $prefix = unpack('P', $area, $offset);
                $bytes = substr($area, $offset + 8, $stored - 8);

                if ($prefix[1] === -1) {
                    $buffers[] = $bytes;

                    continue;
                }

                $decoded = $this->codec->decode($bytes);

                if (strlen($decoded) !== $prefix[1]) {
                    throw new FloeException(sprintf(
                        'Floe BATCH frame buffer %d decodes to %d bytes, its prefix declares %d',
                        $buffer,
                        strlen($decoded),
                        $prefix[1],
                    ));
                }

                $buffers[] = $decoded;
            }

            try {
                // a flat column's only node is its top node, already checked above; the backend checks its buffers
                if ($nodeCounts[$name] === 1) {
                    $node++;
                    $columns[$name] = $backend->decode($definition, $buffers, $rowCount, $nullCount);

                    continue;
                }

                foreach ($this->layout->nodes($type, $length, $nullCount, $buffers) as [
                    $derivedLength,
                    $derivedNulls,
                ]) {
                    if ($words[2 * $node] !== $derivedLength || $words[(2 * $node) + 1] !== $derivedNulls) {
                        throw new FloeException(sprintf(
                            'Floe BATCH frame column "%s" declares nodes that disagree with its buffers',
                            $name,
                        ));
                    }

                    $node++;
                }

                $columns[$name] = $backend->decode($definition, $buffers, $rowCount, $nullCount);
            } catch (InvalidArgumentException $e) {
                throw new FloeException(
                    sprintf('Floe BATCH frame column "%s" is malformed: %s', $name, $e->getMessage()),
                    0,
                    $e,
                );
            }
        }

        try {
            return Rows::fromColumns($schema, $columns, $rowCount);
        } catch (SchemaMismatchException $e) {
            throw new FloeException(sprintf('Floe BATCH frame is malformed: %s', $e->getMessage()), 0, $e);
        }
    }
}
