<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\{config_builder, data_frame, from_array, overwrite};
use function Flow\Filesystem\Bridge\AsyncAWS\DSL\{aws_s3_client, aws_s3_filesystem};
use function Flow\Filesystem\DSL\path;
use Symfony\Component\Dotenv\Dotenv;

require __DIR__ . '/../../../autoload.php';

if (!\file_exists(__DIR__ . '/.env')) {
    print 'Example skipped. Please create .env file with AWS S3 credentials.' . PHP_EOL;

    return;
}

$dotenv = new Dotenv();
$dotenv->load(__DIR__ . '/.env');

$config = config_builder()
    ->mount(
        aws_s3_filesystem(
            $_ENV['AWS_S3_BUCKET'],
            aws_s3_client([
                'region' => $_ENV['AWS_S3_REGION'],
                'accessKeyId' => $_ENV['AWS_S3_KEY'],
                'accessKeySecret' => $_ENV['AWS_S3_SECRET'],
            ])
        )
    );

data_frame($config)
    ->read(from_array([
        ['id' => 1, 'name' => 'test'],
        ['id' => 2, 'name' => 'test'],
        ['id' => 3, 'name' => 'test'],
        ['id' => 4, 'name' => 'test'],
    ]))
    ->saveMode(overwrite())
    ->write(to_csv(path('aws-s3://test.csv')))
    ->run();
