![img](documentation/assets/img/flow_php_banner_02_2022.png)

Flow is a PHP-based, strongly typed data processing framework with a low memory footprint.

[![Latest Stable Version](https://poser.pugx.org/flow-php/flow/v)](https://packagist.org/packages/flow-php/flow)
[![Latest Unstable Version](https://poser.pugx.org/flow-php/flow/v/unstable)](https://packagist.org/packages/flow-php/flow)
[![License](https://poser.pugx.org/flow-php/flow/license)](https://packagist.org/packages/flow-php/flow)
[![Test Suite](https://github.com/flow-php/flow/actions/workflows/test-suite.yml/badge.svg?branch=1.x)](https://github.com/flow-php/flow/actions/workflows/test-suite.yml)

- 📈 [Project Roadmap](https://github.com/orgs/flow-php/projects/1)
- 📜 [Documentation](documentation/introduction.md)
- 🛠️ [Contributing](CONTRIBUTING.md)
- 🚧 [Upgrading](UPGRADE.md)
- <img src="https://cdn.prod.website-files.com/6257adef93867e50d84d30e2/636e0a69f118df70ad7828d4_icon_clyde_blurple_RGB.svg" width="16px" height="16px" alt="Discord"> [Discord Server](https://discord.gg/5dNXfQyACW)

Supported PHP versions: [![PHP 8.1](https://img.shields.io/badge/php-~8.1-8892BF.svg)](https://php.net/) [![PHP 8.2](https://img.shields.io/badge/php-~8.2-8892BF.svg)](https://php.net/) [![PHP 8.3](https://img.shields.io/badge/php-~8.3-8892BF.svg)](https://php.net/) [![PHP 8.4](https://img.shields.io/badge/php-~8.4-8892BF.svg)](https://php.net/)

---

## Flow PHP

Flow is the most advanced PHP ETL (Extract, Transform, Load) framework. 

```php
<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\Parquet\{from_parquet, to_parquet};
use function Flow\ETL\DSL\{data_frame, lit, ref, sum, to_output, overwrite};
use Flow\ETL\Filesystem\SaveMode;

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_parquet(__DIR__ . '/orders_flow.parquet'))
    ->select('created_at', 'total_price', 'discount')
    ->withEntry('created_at', ref('created_at')->cast('date')->dateFormat('Y/m'))
    ->withEntry('revenue', ref('total_price')->minus(ref('discount')))
    ->select('created_at', 'revenue')
    ->groupBy('created_at')
    ->aggregate(sum(ref('revenue')))
    ->sortBy(ref('created_at')->desc())
    ->withEntry('daily_revenue', ref('revenue_sum')->round(lit(2))->numberFormat(lit(2)))
    ->drop('revenue_sum')
    ->write(to_output(truncate: false))
    ->withEntry('created_at', ref('created_at')->toDate('Y/m'))
    ->saveMode(overwrite())
    ->write(to_parquet(__DIR__ . '/daily_revenue.parquet'))
    ->run();
```

```console
$ php daily_revenue.php
+------------+---------------+
| created_at | daily_revenue |
+------------+---------------+
|    2023/10 |    206,669.74 |
|    2023/09 |    227,647.47 |
|    2023/08 |    237,027.31 |
|    2023/07 |    240,111.05 |
|    2023/06 |    225,536.35 |
|    2023/05 |    234,624.74 |
|    2023/04 |    231,472.05 |
|    2023/03 |    231,697.36 |
|    2023/02 |    211,048.97 |
|    2023/01 |    225,539.81 |
+------------+---------------+
10 rows
```

## Community Contributions

Flow PHP is not just a tool, but a growing community of developers passionate about data processing and PHP. We strongly believe in the power of collaboration and welcome contributions of all forms. Whether you're fixing a bug, proposing a new feature, or improving our documentation, your input is invaluable to the growth of Flow PHP.

### How You Can Contribute

- **Submitting Bug Reports and Feature Requests**: Encounter an issue or have an idea for an enhancement? Submit an issue on our GitHub repository. Please provide a clear description and, if possible, steps to reproduce the bug or details of the feature request.
- **Code Contributions**: Interested in directly impacting the development of Flow PHP? Check out our issue tracker for areas where you can contribute. From simple fixes to substantial feature additions, every bit of help is appreciated.
- **Improving Documentation**: Good documentation is key to any project's success. If you notice gaps, inaccuracies, or areas that could use better explanations, we encourage you to submit updates.
- **Community Support**: Help out fellow users by answering questions on our community channels, Stack Overflow, or other forums where Flow PHP users gather.
- **Spread the Word**: Share your experiences using Flow PHP, write blog posts, tutorials, or speak at meetups and conferences. Let others know how Flow PHP has helped in your projects!
- **Leave a GitHub Star**: If you find Flow PHP useful, consider giving it a star on GitHub. Your star is a simple yet powerful way to show support and helps others discover our project.

### Contribution Guidelines

To ensure a smooth collaboration process, we've put together guidelines for contributing. 
Please take a moment to read our [Contribution Guidelines](CONTRIBUTING.md) before starting your work. This will help you understand our process and make contributing a breeze.

### Questions?

If you have any questions about contributing, please feel free to reach out to us. We're more than happy to provide guidance and support.

Join us in shaping the future of data processing in PHP — every contribution, big or small, makes a significant difference!

## GitHub Stars

[![Star History Chart](https://api.star-history.com/svg?repos=flow-php/flow&type=Date)](https://star-history.com/#flow-php/flow&Date)

## Sponsors

Flow PHP is sponsored by:

- [Blackfire](https://blackfire.io/) - the best PHP profiling and monitoring tool! 

