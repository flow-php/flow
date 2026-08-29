<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\DSL;

use Flow\ETL\Attribute\DocumentationDSL;
use Flow\ETL\Attribute\Module;
use Flow\ETL\Attribute\Type;
use Flow\Filesystem\Bridge\SFTP\Options;
use Flow\Filesystem\Bridge\SFTP\SFTPFilesystem;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Mount;
use phpseclib3\Crypt\Common\PrivateKey;
use phpseclib3\Net\SFTP;
use SensitiveParameter;

use function sprintf;

/**
 * @throws RuntimeException
 */
#[DocumentationDSL(module: Module::SFTP_FILESYSTEM, type: Type::HELPER)]
function sftp_client(
    string $host,
    string $user,
    #[SensitiveParameter]
    string|PrivateKey $credential,
    int $port = 22,
): SFTP {
    $sftp = new SFTP($host, $port);

    if (!$sftp->login($user, $credential)) {
        throw new RuntimeException(sprintf(
            'Failed to authenticate user "%s" against sftp://%s:%d',
            $user,
            $host,
            $port,
        ));
    }

    return $sftp;
}

#[DocumentationDSL(module: Module::SFTP_FILESYSTEM, type: Type::HELPER)]
function sftp_filesystem(SFTP $sftp, Options $options = new Options(), string $protocol = 'sftp'): SFTPFilesystem
{
    return new SFTPFilesystem(new Mount($protocol), $sftp, $options);
}

#[DocumentationDSL(module: Module::SFTP_FILESYSTEM, type: Type::HELPER)]
function sftp_filesystem_options(): Options
{
    return new Options();
}
