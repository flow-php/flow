<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\{data_frame, from_array, overwrite};
use Flow\ETL\Adapter\Filesystem\AwsS3Stream;
use Flow\ETL\Filesystem\Path;
use Symfony\Component\Dotenv\Dotenv;

require __DIR__ . '/../../../autoload.php';

if (!\file_exists(__DIR__ . '/.env')) {
    print 'Example skipped. Please create .env file with AWS S3 credentials.' . PHP_EOL;

    return;
}

$dotenv = new Dotenv();
$dotenv->load(__DIR__ . '/.env');

$s3_client_option = [
    'client' => [
        'credentials' => [
            'key' => $_ENV['AWS_S3_KEY'],
            'secret' => $_ENV['AWS_S3_SECRET'],
        ],
        'region' => 'eu-west-2',
        'version' => 'latest',
    ],
    'bucket' => 'flow-php',
];

AwsS3Stream::register();

data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'test'],
        ['id' => 2, 'name' => 'test'],
        ['id' => 3, 'name' => 'test'],
        ['id' => 4, 'name' => 'test'],
    ]))
    ->saveMode(overwrite())
    ->write(to_csv(new Path('flow-aws-s3://test.csv', $s3_client_option)))
    ->run();
