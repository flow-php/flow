<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JSONMachine;

/**
 * How a file carries its records: one JSON text whose members are the rows, or one JSON text per line
 * (https://jsonlines.org).
 */
enum JsonFormat
{
    case Document;
    case Lines;
}
