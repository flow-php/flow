<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\IO\Loader;

use Flow\ETL\IO\Loader\File\FileWriter;
use Flow\ETL\Values\Rows;
use Flow\Filesystem\DestinationStream;

final class XMLFileWriter implements FileWriter
{
    private bool $written = false;

    /**
     * @param string $head the XML declaration and the opening root element, written before the first rows
     */
    public function __construct(
        private readonly DestinationStream $stream,
        private readonly XMLEncoder $encoder,
        private readonly string $head,
        private readonly string $rootElementName,
    ) {}

    /**
     * Closes the root element the first write opened; a sink that wrote nothing appends nothing.
     */
    public function close(): void
    {
        if ($this->written) {
            $this->stream->append('</' . $this->rootElementName . '>');
        }
    }

    public function write(Rows $rows): void
    {
        $this->stream->append(($this->written ? '' : $this->head) . $this->encoder->encode($rows));
        $this->written = true;
    }
}
