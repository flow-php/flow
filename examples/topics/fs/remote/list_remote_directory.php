<?php declare(strict_types=1);

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    print "This example cannot be run in PHAR, please use CLI approach.\n";

    exit(1);
}

use function Flow\ETL\Adapter\Filesystem\remote_files;
use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\path;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_output;
use Flow\ETL\Adapter\Filesystem\AzureBlobStream;
use Symfony\Component\Dotenv\Dotenv;

require __DIR__ . '/../../../../vendor/autoload.php';

$dotenv = new Dotenv();
$dotenv->load(__DIR__ . '/../../../.env');

$blob_account = $_ENV['AZURE_BLOB_ACCOUNT'];
$blob_key = $_ENV['AZURE_BLOB_KEY'];

$azure_blob_config = [
    'connection-string' => "DefaultEndpointsProtocol=https;AccountName={$blob_account};AccountKey={$blob_key}",
    'container' => 'flow-php',
];

AzureBlobStream::register();

data_frame()
    ->read(remote_files(path('flow-azure-blob://docs', $azure_blob_config), true))
    ->collect()
    ->filter(ref('is_file')->isTrue())
    ->withEntry('size', concat(ref('size'), lit(' bytes')))
    ->select('scheme', 'uri', 'size', 'last_modified')
    ->withEntry('last_modified', ref('last_modified')->cast('datetime')->dateFormat('Y-m-d H:i:s'))
    ->write(to_output(false))
    ->run();
