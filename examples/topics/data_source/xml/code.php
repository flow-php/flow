<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_output;

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_xml(
        __DIR__ . '/input/dataset.xml',
        'users/user'
    ))
    ->withEntry('id', ref('node')->xpath('@id')->domNodeValue())
    ->withEntry('name', ref('node')->xpath('name')->domNodeValue())
    ->withEntry('active', ref('node')->xpath('active')->domNodeValue())
    ->withEntry('email', ref('node')->xpath('email')->domNodeValue())
    ->drop('node')
    ->write(to_output(false))
    ->run();
