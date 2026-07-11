<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Rows;
use Flow\Floe\Codec\NoopCodec;

use function chr;
use function pack;
use function strlen;

final class PhpRowFrameEncoder implements RowFrameEncoder
{
    private ?EncoderPlan $plan = null;

    public function __construct(
        private readonly Codec $codec = new NoopCodec(),
        private readonly RowEncoder $rowEncoder = new RowEncoder(),
        private readonly SchemaTracker $schemaTracker = new SchemaTracker(),
    ) {}

    public function encode(Rows $rows): array
    {
        $segments = [];
        $schemaBody = null;
        $frames = '';
        $rowCount = 0;

        foreach ($rows->all() as $row) {
            if ($this->plan === null || !$this->schemaTracker->fits($this->plan, $row)) {
                if ($rowCount > 0 || $schemaBody !== null) {
                    $segments[] = new FrameSegment($schemaBody, $frames, $rowCount);
                }

                $this->plan = FloeWriter::growSectionPlan($this->plan?->schemaBody, $row);
                $schemaBody = $this->plan->schemaBody;
                $frames = '';
                $rowCount = 0;
            }

            $body = $this->codec->encode($this->rowEncoder->encode($this->plan, $row));

            $frames .= chr(Format::FRAME_ROW) . pack('V', strlen($body)) . $body;
            $rowCount++;
        }

        if ($rowCount > 0) {
            $segments[] = new FrameSegment($schemaBody, $frames, $rowCount);
        }

        return $segments;
    }
}
