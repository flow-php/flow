<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Row\Encoder;
use Flow\ETL\Row\Hydrator;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Floe\Exception\FloeException;

/**
 * @extends Encoder<string>
 */
interface FloeEncoder extends Encoder
{
    /**
     * `$hydrator->hydrate($this->decode($bodies), $schema)`, in one native pass where the hydrator is native too.
     *
     * @param list<string> $bodies
     *
     * @throws FloeException
     * @throws SchemaMismatchException
     */
    public function decodeRows(array $bodies, Schema $schema, Hydrator $hydrator): Rows;

    /**
     * `Format::rowFrames($this->encode($hydrator->dehydrate($rows)))`, in one native pass where the hydrator is native
     * too - complete ROW frames for a writer whose codec leaves bodies as they are.
     *
     * @throws FloeException
     * @throws SchemaMismatchException
     */
    public function encodeFrames(Rows $rows, Hydrator $hydrator): string;
}
