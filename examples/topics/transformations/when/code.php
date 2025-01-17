<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, lit, ref, to_stream, when};

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_array([
        ['id' => 1, 'email' => 'user01@flow-php.com', 'active' => true, 'tags' => ['foo', 'bar']],
        ['id' => 2, 'email' => 'user02@flow-php.com', 'active' => false, 'tags' => ['biz', 'bar']],
        ['id' => 3, 'email' => 'user03@flow-php.com', 'active' => true, 'tags' => ['bar', 'baz']],
    ]))
    ->collect()
    ->withEntry(
        'is_special',
        when(
            ref('active')->isTrue()->and(ref('tags')->contains('foo')),
            lit(true),
            lit(false)
        )
    )
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
