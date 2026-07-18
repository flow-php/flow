<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\Filesystem\SourceStream;
use Flow\Floe\Exception\FloeException;

use function sprintf;

final readonly class FooterReader
{
    /**
     * @throws FloeException
     */
    public function read(SourceStream $source, Codec $codec): FooterLocation
    {
        $uri = $source->path()->uri();
        $size = $source->size();

        if ($size === null) {
            throw new FloeException(sprintf(
                'Floe footer requires a sized stream, "%s" does not report its size',
                $uri,
            ));
        }

        if ($size < (Format::HEADER_LENGTH + Format::TRAILER_LENGTH)) {
            throw new FloeException(sprintf('Floe file "%s" is torn, too small to hold a header and a trailer', $uri));
        }

        $flags = Format::validateHeader($source->read(Format::HEADER_LENGTH, 0));

        if ($flags !== $codec->id()) {
            throw new FloeException(sprintf(
                'Floe file "%s" was written with codec 0x%02X, expected 0x%02X',
                $uri,
                $flags,
                $codec->id(),
            ));
        }

        $footerLength = Format::parseTrailer($source->read(Format::TRAILER_LENGTH, $size - Format::TRAILER_LENGTH));

        if (($size - Format::TRAILER_LENGTH - $footerLength - Format::FRAME_HEADER_LENGTH) < Format::HEADER_LENGTH) {
            throw new FloeException(sprintf('Floe file "%s" is torn, footer does not fit inside the file', $uri));
        }

        /** @var int<1, max> $footerLength */
        $footerFrameStart = $size - Format::TRAILER_LENGTH - $footerLength - Format::FRAME_HEADER_LENGTH;

        return new FooterLocation(
            Footer::fromJson($source->read($footerLength, $size - Format::TRAILER_LENGTH - $footerLength)),
            $footerFrameStart,
        );
    }
}
