<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Floe\Exception\FloeException;

interface FloeEncoder
{
    /**
     * `RowsBuilder` over `decode()`.
     *
     * @param list<string> $bodies
     *
     * @throws FloeException
     * @throws SchemaMismatchException
     */
    public function decodeRows(array $bodies, Schema $schema): Rows;

    /**
     * `Format::rowFrames($this->encode($rows))`.
     *
     * @throws FloeException
     * @throws SchemaMismatchException
     */
    public function encodeFrames(Rows $rows): string;
}
