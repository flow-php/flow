/**
 * ACE Editor Completer for Flow PHP DSL Functions
 *
 * Auto-generated on 2025\u002D11\u002D04\u002020\u003A36\u003A18
 * Total functions: 341
 *
 * This completer provides autocompletion for all Flow PHP DSL functions:
 * - Extractors (flow-extractors)
 * - Loaders (flow-loaders)
 * - Transformers (flow-transformers)
 * - Scalar Functions (flow-scalar-functions)
 * - Aggregating Functions (flow-aggregating-functions)
 * - Window Functions (flow-window-functions)
 * - And more...
 */

ace.define("ace/completers/flow_dsl", ["require", "exports", "module"], function(require, exports, module) {
    "use strict";

    var snippetManager = require("ace/snippets").snippetManager;

    var flowDslCompleter = {
        getCompletions: function(editor, session, pos, prefix, callback) {
            if (session.$mode && session.$mode.$id !== 'ace/mode/php') {
                callback(null, []);
                return;
            }

            // Don't show completions inside strings
            var token = session.getTokenAt(pos.row, pos.column);
            if (token && (token.type === 'string' || token.type.indexOf('string') !== -1)) {
                callback(null, []);
                return;
            }

            var allCompletions = [
                                {
                    name: "add_row_index",
                    caption: "add_row_index",
                    snippet: "\\Flow\\ETL\\DSL\\add_row_index(${1:string $column}, ${2:StartFrom $startFrom})",
                    meta: "flow-dsl-transformers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">add_row_index</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$column</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">StartFrom</span> <span class=\"fn-param\">$startFrom</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AddRowIndex</span></code></pre>"

                },
                                {
                    name: "all",
                    caption: "all",
                    snippet: "\\Flow\\ETL\\DSL\\all(${1:ScalarFunction $functions})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">all</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$functions</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">All</span></code></pre>"

                },
                                {
                    name: "analyze",
                    caption: "analyze",
                    snippet: "\\Flow\\ETL\\DSL\\analyze()",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">analyze</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Analyze</span></code></pre>"

                },
                                {
                    name: "any",
                    caption: "any",
                    snippet: "\\Flow\\ETL\\DSL\\any(${1:ScalarFunction $values})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">any</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$values</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Any</span></code></pre>"

                },
                                {
                    name: "append",
                    caption: "append",
                    snippet: "\\Flow\\ETL\\DSL\\append()",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">append</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SaveMode</span></code></pre>"

                },
                                {
                    name: "array_exists",
                    caption: "array_exists",
                    snippet: "\\Flow\\ETL\\DSL\\array_exists(${1:ScalarFunction|array $ref}, ${2:ScalarFunction|string $path})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">array_exists</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayPathExists</span></code></pre><p>@param array<array-key, mixed>|ScalarFunction $ref</p>"

                },
                                {
                    name: "array_expand",
                    caption: "array_expand",
                    snippet: "\\Flow\\ETL\\DSL\\array_expand(${1:ScalarFunction $function}, ${2:ArrayExpand $expand})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">array_expand</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$function</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ArrayExpand</span> <span class=\"fn-param\">$expand</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayExpand</span></code></pre><p>Expands each value into entry, if there are more than one value, multiple rows will be created.<br>Array keys are ignored, only values are used to create new rows.<br>Before:<br>  +--+-------------------+<br>  |id|              array|<br>  +--+-------------------+<br>  | 1|{\"a\":1,\"b\":2,\"c\":3}|<br>  +--+-------------------+<br>After:<br>  +--+--------+<br>  |id|expanded|<br>  +--+--------+<br>  | 1|       1|<br>  | 1|       2|<br>  | 1|       3|<br>  +--+--------+</p>"

                },
                                {
                    name: "array_get",
                    caption: "array_get",
                    snippet: "\\Flow\\ETL\\DSL\\array_get(${1:ScalarFunction $ref}, ${2:ScalarFunction|string $path})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">array_get</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayGet</span></code></pre>"

                },
                                {
                    name: "array_get_collection",
                    caption: "array_get_collection",
                    snippet: "\\Flow\\ETL\\DSL\\array_get_collection(${1:ScalarFunction $ref}, ${2:ScalarFunction|array $keys})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">array_get_collection</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$keys</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayGetCollection</span></code></pre><p>@param array<array-key, mixed>|ScalarFunction $keys</p>"

                },
                                {
                    name: "array_get_collection_first",
                    caption: "array_get_collection_first",
                    snippet: "\\Flow\\ETL\\DSL\\array_get_collection_first(${1:ScalarFunction $ref}, ${2:string $keys})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">array_get_collection_first</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$keys</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayGetCollection</span></code></pre>"

                },
                                {
                    name: "array_keys_style_convert",
                    caption: "array_keys_style_convert",
                    snippet: "\\Flow\\ETL\\DSL\\array_keys_style_convert(${1:ScalarFunction $ref}, ${2:StringStyles|StringStyles|string $style})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">array_keys_style_convert</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">StringStyles|StringStyles|string</span> <span class=\"fn-param\">$style</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayKeysStyleConvert</span></code></pre>"

                },
                                {
                    name: "array_key_rename",
                    caption: "array_key_rename",
                    snippet: "\\Flow\\ETL\\DSL\\array_key_rename(${1:ScalarFunction $ref}, ${2:ScalarFunction|string $path}, ${3:ScalarFunction|string $newName})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">array_key_rename</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$newName</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayKeyRename</span></code></pre>"

                },
                                {
                    name: "array_merge",
                    caption: "array_merge",
                    snippet: "\\Flow\\ETL\\DSL\\array_merge(${1:ScalarFunction|array $left}, ${2:ScalarFunction|array $right})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">array_merge</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$right</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayMerge</span></code></pre><p>@param array<array-key, mixed>|ScalarFunction $left<br>@param array<array-key, mixed>|ScalarFunction $right</p>"

                },
                                {
                    name: "array_merge_collection",
                    caption: "array_merge_collection",
                    snippet: "\\Flow\\ETL\\DSL\\array_merge_collection(${1:ScalarFunction|array $array})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">array_merge_collection</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$array</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayMergeCollection</span></code></pre><p>@param array<array-key, mixed>|ScalarFunction $array</p>"

                },
                                {
                    name: "array_reverse",
                    caption: "array_reverse",
                    snippet: "\\Flow\\ETL\\DSL\\array_reverse(${1:ScalarFunction|array $function}, ${2:ScalarFunction|bool $preserveKeys})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">array_reverse</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$function</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|bool</span> <span class=\"fn-param\">$preserveKeys</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayReverse</span></code></pre><p>@param array<array-key, mixed>|ScalarFunction $function</p>"

                },
                                {
                    name: "array_sort",
                    caption: "array_sort",
                    snippet: "\\Flow\\ETL\\DSL\\array_sort(${1:ScalarFunction $function}, ${2:ScalarFunction|Sort|null $sort_function}, ${3:ScalarFunction|int|null $flags}, ${4:ScalarFunction|bool $recursive})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">array_sort</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$function</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|Sort|null</span> <span class=\"fn-param\">$sort_function</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int|null</span> <span class=\"fn-param\">$flags</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|bool</span> <span class=\"fn-param\">$recursive</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArraySort</span></code></pre>"

                },
                                {
                    name: "array_to_generator",
                    caption: "array_to_generator",
                    snippet: "\\Flow\\ETL\\Adapter\\Parquet\\array_to_generator(${1:array $data})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">array_to_generator</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$data</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Generator</span></code></pre><p>@template T<br>@param array<T> $data<br>@return \Generator<T></p>"

                },
                                {
                    name: "array_to_row",
                    caption: "array_to_row",
                    snippet: "\\Flow\\ETL\\DSL\\array_to_row(${1:array $data}, ${2:EntryFactory $entryFactory}, ${3:Partitions|array $partitions}, ${4:Schema $schema})",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">array_to_row</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$data</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">EntryFactory</span> <span class=\"fn-param\">$entryFactory</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Partitions|array</span> <span class=\"fn-param\">$partitions</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Row</span></code></pre><p>@param array<array<mixed>>|array<mixed|string> $data<br>@param array<Partition>|Partitions $partitions<br>@param null|Schema $schema</p>"

                },
                                {
                    name: "array_to_rows",
                    caption: "array_to_rows",
                    snippet: "\\Flow\\ETL\\DSL\\array_to_rows(${1:array $data}, ${2:EntryFactory $entryFactory}, ${3:Partitions|array $partitions}, ${4:Schema $schema})",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">array_to_rows</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$data</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">EntryFactory</span> <span class=\"fn-param\">$entryFactory</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Partitions|array</span> <span class=\"fn-param\">$partitions</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Rows</span></code></pre><p>@param array<array<mixed>>|array<mixed|string> $data<br>@param array<Partition>|Partitions $partitions<br>@param null|Schema $schema</p>"

                },
                                {
                    name: "array_unpack",
                    caption: "array_unpack",
                    snippet: "\\Flow\\ETL\\DSL\\array_unpack(${1:ScalarFunction|array $array}, ${2:ScalarFunction|array $skip_keys}, ${3:ScalarFunction|string|null $entry_prefix})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">array_unpack</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$array</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$skip_keys</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string|null</span> <span class=\"fn-param\">$entry_prefix</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayUnpack</span></code></pre><p>@param array<array-key, mixed>|ScalarFunction $array<br>@param array<array-key, mixed>|ScalarFunction $skip_keys</p>"

                },
                                {
                    name: "average",
                    caption: "average",
                    snippet: "\\Flow\\ETL\\DSL\\average(${1:EntryReference|string $ref}, ${2:int $scale}, ${3:Rounding $rounding})",
                    meta: "flow-dsl-aggregating-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">average</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$scale</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Rounding</span> <span class=\"fn-param\">$rounding</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Average</span></code></pre>"

                },
                                {
                    name: "aws_s3_client",
                    caption: "aws_s3_client",
                    snippet: "\\Flow\\Filesystem\\Bridge\\AsyncAWS\\DSL\\aws_s3_client(${1:array $configuration})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">aws_s3_client</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$configuration</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">S3Client</span></code></pre><p>@param array<string, mixed> $configuration - for details please see https://async-aws.com/clients/s3.html</p>"

                },
                                {
                    name: "aws_s3_filesystem",
                    caption: "aws_s3_filesystem",
                    snippet: "\\Flow\\Filesystem\\Bridge\\AsyncAWS\\DSL\\aws_s3_filesystem(${1:string $bucket}, ${2:S3Client $s3Client}, ${3:Options $options})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">aws_s3_filesystem</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$bucket</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">S3Client</span> <span class=\"fn-param\">$s3Client</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Options</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AsyncAWSS3Filesystem</span></code></pre>"

                },
                                {
                    name: "azure_blob_service",
                    caption: "azure_blob_service",
                    snippet: "\\Flow\\Azure\\SDK\\DSL\\azure_blob_service(${1:Configuration $configuration}, ${2:AuthorizationFactory $azure_authorization_factory}, ${3:ClientInterface $client}, ${4:HttpFactory $azure_http_factory}, ${5:URLFactory $azure_url_factory}, ${6:LoggerInterface $logger})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">azure_blob_service</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Configuration</span> <span class=\"fn-param\">$configuration</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">AuthorizationFactory</span> <span class=\"fn-param\">$azure_authorization_factory</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ClientInterface</span> <span class=\"fn-param\">$client</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">HttpFactory</span> <span class=\"fn-param\">$azure_http_factory</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">URLFactory</span> <span class=\"fn-param\">$azure_url_factory</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">LoggerInterface</span> <span class=\"fn-param\">$logger</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BlobServiceInterface</span></code></pre>"

                },
                                {
                    name: "azure_blob_service_config",
                    caption: "azure_blob_service_config",
                    snippet: "\\Flow\\Azure\\SDK\\DSL\\azure_blob_service_config(${1:string $account}, ${2:string $container})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">azure_blob_service_config</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$account</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$container</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Configuration</span></code></pre>"

                },
                                {
                    name: "azure_filesystem",
                    caption: "azure_filesystem",
                    snippet: "\\Flow\\Filesystem\\Bridge\\Azure\\DSL\\azure_filesystem(${1:BlobServiceInterface $blob_service}, ${2:Options $options})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">azure_filesystem</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">BlobServiceInterface</span> <span class=\"fn-param\">$blob_service</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Options</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AzureBlobFilesystem</span></code></pre>"

                },
                                {
                    name: "azure_filesystem_options",
                    caption: "azure_filesystem_options",
                    snippet: "\\Flow\\Filesystem\\Bridge\\Azure\\DSL\\azure_filesystem_options()",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">azure_filesystem_options</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Options</span></code></pre>"

                },
                                {
                    name: "azure_http_factory",
                    caption: "azure_http_factory",
                    snippet: "\\Flow\\Azure\\SDK\\DSL\\azure_http_factory(${1:RequestFactoryInterface $request_factory}, ${2:StreamFactoryInterface $stream_factory})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">azure_http_factory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">RequestFactoryInterface</span> <span class=\"fn-param\">$request_factory</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">StreamFactoryInterface</span> <span class=\"fn-param\">$stream_factory</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">HttpFactory</span></code></pre>"

                },
                                {
                    name: "azure_shared_key_authorization_factory",
                    caption: "azure_shared_key_authorization_factory",
                    snippet: "\\Flow\\Azure\\SDK\\DSL\\azure_shared_key_authorization_factory(${1:string $account}, ${2:string $key})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">azure_shared_key_authorization_factory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$account</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$key</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SharedKeyFactory</span></code></pre>"

                },
                                {
                    name: "azure_url_factory",
                    caption: "azure_url_factory",
                    snippet: "\\Flow\\Azure\\SDK\\DSL\\azure_url_factory(${1:string $host})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">azure_url_factory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$host</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AzureURLFactory</span></code></pre>"

                },
                                {
                    name: "azurite_url_factory",
                    caption: "azurite_url_factory",
                    snippet: "\\Flow\\Azure\\SDK\\DSL\\azurite_url_factory(${1:string $host}, ${2:string $port}, ${3:bool $secure})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">azurite_url_factory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$host</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$port</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$secure</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AzuriteURLFactory</span></code></pre>"

                },
                                {
                    name: "bar_chart",
                    caption: "bar_chart",
                    snippet: "\\Flow\\ETL\\Adapter\\ChartJS\\bar_chart(${1:EntryReference $label}, ${2:References $datasets})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">bar_chart</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference</span> <span class=\"fn-param\">$label</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">References</span> <span class=\"fn-param\">$datasets</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BarChart</span></code></pre>"

                },
                                {
                    name: "batched_by",
                    caption: "batched_by",
                    snippet: "\\Flow\\ETL\\DSL\\batched_by(${1:Extractor $extractor}, ${2:Reference|string $column}, ${3:int $min_size})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">batched_by</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Extractor</span> <span class=\"fn-param\">$extractor</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$column</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$min_size</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BatchByExtractor</span></code></pre><p>@param null|int<1, max> $min_size</p>"

                },
                                {
                    name: "batches",
                    caption: "batches",
                    snippet: "\\Flow\\ETL\\DSL\\batches(${1:Extractor $extractor}, ${2:int $size})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">batches</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Extractor</span> <span class=\"fn-param\">$extractor</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$size</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BatchExtractor</span></code></pre><p>@param int<1, max> $size</p>"

                },
                                {
                    name: "batch_size",
                    caption: "batch_size",
                    snippet: "\\Flow\\ETL\\DSL\\batch_size(${1:int $size})",
                    meta: "flow-dsl-transformers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">batch_size</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$size</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BatchSize</span></code></pre><p>@param int<1, max> $size</p>"

                },
                                {
                    name: "between",
                    caption: "between",
                    snippet: "\\Flow\\ETL\\DSL\\between(${1:mixed $value}, ${2:mixed $lower_bound}, ${3:mixed $upper_bound}, ${4:ScalarFunction|Boundary $boundary})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">between</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$lower_bound</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$upper_bound</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|Boundary</span> <span class=\"fn-param\">$boundary</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Between</span></code></pre>"

                },
                                {
                    name: "boolean_entry",
                    caption: "boolean_entry",
                    snippet: "\\Flow\\ETL\\DSL\\boolean_entry(${1:string $name}, ${2:bool $value}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-entries",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">boolean_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span></code></pre><p>@return Entry<?bool></p>"

                },
                                {
                    name: "bool_entry",
                    caption: "bool_entry",
                    snippet: "\\Flow\\ETL\\DSL\\bool_entry(${1:string $name}, ${2:bool $value}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-entries",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">bool_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span></code></pre><p>@return Entry<?bool></p>"

                },
                                {
                    name: "bool_schema",
                    caption: "bool_schema",
                    snippet: "\\Flow\\ETL\\DSL\\bool_schema(${1:string $name}, ${2:bool $nullable}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-schema",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">bool_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span></code></pre><p>@return Definition<bool></p>"

                },
                                {
                    name: "call",
                    caption: "call",
                    snippet: "\\Flow\\ETL\\DSL\\call(${1:ScalarFunction|callable $callable}, ${2:array $parameters}, ${3:Type $return_type})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">call</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|callable</span> <span class=\"fn-param\">$callable</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$parameters</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$return_type</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CallUserFunc</span></code></pre><p>Calls a user-defined function with the given parameters.<br>@param callable|ScalarFunction $callable<br>@param array<mixed> $parameters<br>@param null|Type<mixed> $return_type</p>"

                },
                                {
                    name: "capitalize",
                    caption: "capitalize",
                    snippet: "\\Flow\\ETL\\DSL\\capitalize(${1:ScalarFunction|string $value})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">capitalize</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Capitalize</span></code></pre>"

                },
                                {
                    name: "cast",
                    caption: "cast",
                    snippet: "\\Flow\\ETL\\DSL\\cast(${1:mixed $value}, ${2:Type|string $type})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">cast</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type|string</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Cast</span></code></pre><p>@param \Flow\Types\Type<mixed>|string $type</p>"

                },
                                {
                    name: "chunks_from",
                    caption: "chunks_from",
                    snippet: "\\Flow\\ETL\\DSL\\chunks_from(${1:Extractor $extractor}, ${2:int $chunk_size})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">chunks_from</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Extractor</span> <span class=\"fn-param\">$extractor</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$chunk_size</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BatchExtractor</span></code></pre><p>@param int<1, max> $chunk_size<br>@deprecated use batches() instead</p>"

                },
                                {
                    name: "coalesce",
                    caption: "coalesce",
                    snippet: "\\Flow\\ETL\\DSL\\coalesce(${1:ScalarFunction $values})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">coalesce</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$values</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Coalesce</span></code></pre>"

                },
                                {
                    name: "col",
                    caption: "col",
                    snippet: "\\Flow\\ETL\\DSL\\col(${1:string $entry})",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">col</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">EntryReference</span></code></pre><p>An alias for `ref`.</p>"

                },
                                {
                    name: "collect",
                    caption: "collect",
                    snippet: "\\Flow\\ETL\\DSL\\collect(${1:EntryReference|string $ref})",
                    meta: "flow-dsl-aggregating-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">collect</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Collect</span></code></pre>"

                },
                                {
                    name: "collect_unique",
                    caption: "collect_unique",
                    snippet: "\\Flow\\ETL\\DSL\\collect_unique(${1:EntryReference|string $ref})",
                    meta: "flow-dsl-aggregating-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">collect_unique</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CollectUnique</span></code></pre>"

                },
                                {
                    name: "combine",
                    caption: "combine",
                    snippet: "\\Flow\\ETL\\DSL\\combine(${1:ScalarFunction|array $keys}, ${2:ScalarFunction|array $values})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">combine</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$keys</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$values</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Combine</span></code></pre><p>@param array<array-key, mixed>|ScalarFunction $keys<br>@param array<array-key, mixed>|ScalarFunction $values</p>"

                },
                                {
                    name: "compare_all",
                    caption: "compare_all",
                    snippet: "\\Flow\\ETL\\DSL\\compare_all(${1:Comparison $comparisons})",
                    meta: "flow-dsl-comparisons",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">compare_all</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Comparison</span> <span class=\"fn-param\">$comparisons</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">All</span></code></pre>"

                },
                                {
                    name: "compare_any",
                    caption: "compare_any",
                    snippet: "\\Flow\\ETL\\DSL\\compare_any(${1:Comparison $comparisons})",
                    meta: "flow-dsl-comparisons",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">compare_any</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Comparison</span> <span class=\"fn-param\">$comparisons</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Any</span></code></pre>"

                },
                                {
                    name: "compare_entries_by_name",
                    caption: "compare_entries_by_name",
                    snippet: "\\Flow\\ETL\\DSL\\compare_entries_by_name(${1:Order $order})",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">compare_entries_by_name</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Order</span> <span class=\"fn-param\">$order</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Comparator</span></code></pre>"

                },
                                {
                    name: "compare_entries_by_name_desc",
                    caption: "compare_entries_by_name_desc",
                    snippet: "\\Flow\\ETL\\DSL\\compare_entries_by_name_desc()",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">compare_entries_by_name_desc</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Comparator</span></code></pre>"

                },
                                {
                    name: "compare_entries_by_type",
                    caption: "compare_entries_by_type",
                    snippet: "\\Flow\\ETL\\DSL\\compare_entries_by_type(${1:array $priorities}, ${2:Order $order})",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">compare_entries_by_type</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$priorities</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Order</span> <span class=\"fn-param\">$order</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Comparator</span></code></pre><p>@param array<class-string<Entry<mixed>>, int> $priorities</p>"

                },
                                {
                    name: "compare_entries_by_type_and_name",
                    caption: "compare_entries_by_type_and_name",
                    snippet: "\\Flow\\ETL\\DSL\\compare_entries_by_type_and_name(${1:array $priorities}, ${2:Order $order})",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">compare_entries_by_type_and_name</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$priorities</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Order</span> <span class=\"fn-param\">$order</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Comparator</span></code></pre><p>@param array<class-string<Entry<mixed>>, int> $priorities</p>"

                },
                                {
                    name: "compare_entries_by_type_desc",
                    caption: "compare_entries_by_type_desc",
                    snippet: "\\Flow\\ETL\\DSL\\compare_entries_by_type_desc(${1:array $priorities})",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">compare_entries_by_type_desc</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$priorities</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Comparator</span></code></pre><p>@param array<class-string<Entry<mixed>>, int> $priorities</p>"

                },
                                {
                    name: "concat",
                    caption: "concat",
                    snippet: "\\Flow\\ETL\\DSL\\concat(${1:ScalarFunction|string $functions})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">concat</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$functions</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Concat</span></code></pre><p>Concat all values. If you want to concatenate values with separator use concat_ws function.</p>"

                },
                                {
                    name: "concat_ws",
                    caption: "concat_ws",
                    snippet: "\\Flow\\ETL\\DSL\\concat_ws(${1:ScalarFunction|string $separator}, ${2:ScalarFunction|string $functions})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">concat_ws</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$separator</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$functions</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ConcatWithSeparator</span></code></pre><p>Concat all values with separator.</p>"

                },
                                {
                    name: "config",
                    caption: "config",
                    snippet: "\\Flow\\ETL\\DSL\\config()",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">config</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Config</span></code></pre>"

                },
                                {
                    name: "config_builder",
                    caption: "config_builder",
                    snippet: "\\Flow\\ETL\\DSL\\config_builder()",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">config_builder</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ConfigBuilder</span></code></pre>"

                },
                                {
                    name: "constraint_sorted_by",
                    caption: "constraint_sorted_by",
                    snippet: "\\Flow\\ETL\\DSL\\constraint_sorted_by(${1:Reference|string $column}, ${2:Reference|string $columns})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">constraint_sorted_by</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$column</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$columns</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SortedByConstraint</span></code></pre>"

                },
                                {
                    name: "constraint_unique",
                    caption: "constraint_unique",
                    snippet: "\\Flow\\ETL\\DSL\\constraint_unique(${1:string $reference}, ${2:string $references})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">constraint_unique</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$reference</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$references</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">UniqueConstraint</span></code></pre>"

                },
                                {
                    name: "count",
                    caption: "count",
                    snippet: "\\Flow\\ETL\\DSL\\count(${1:EntryReference $function})",
                    meta: "flow-dsl-aggregating-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">count</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference</span> <span class=\"fn-param\">$function</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Count</span></code></pre>"

                },
                                {
                    name: "csv_detect_separator",
                    caption: "csv_detect_separator",
                    snippet: "\\Flow\\ETL\\Adapter\\CSV\\csv_detect_separator(${1:SourceStream $stream}, ${2:int $lines}, ${3:Option $fallback}, ${4:Options $options})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">csv_detect_separator</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">SourceStream</span> <span class=\"fn-param\">$stream</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$lines</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Option</span> <span class=\"fn-param\">$fallback</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Options</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Option</span></code></pre><p>@param SourceStream $stream - valid resource to CSV file<br>@param int<1, max> $lines - number of lines to read from CSV file, default 5, more lines means more accurate detection but slower detection<br>@param null|Option $fallback - fallback option to use when no best option can be detected, default is Option(\',\', \'\"\', \'\\\')<br>@param null|Options $options - options to use for detection, default is Options::all()</p>"

                },
                                {
                    name: "data_frame",
                    caption: "data_frame",
                    snippet: "\\Flow\\ETL\\DSL\\data_frame(${1:Config|ConfigBuilder|null $config})",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">data_frame</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Config|ConfigBuilder|null</span> <span class=\"fn-param\">$config</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Flow</span></code></pre>"

                },
                                {
                    name: "datetime_entry",
                    caption: "datetime_entry",
                    snippet: "\\Flow\\ETL\\DSL\\datetime_entry(${1:string $name}, ${2:DateTimeInterface|string|null $value}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-entries",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">datetime_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DateTimeInterface|string|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span></code></pre><p>@return Entry<?\DateTimeInterface></p>"

                },
                                {
                    name: "datetime_schema",
                    caption: "datetime_schema",
                    snippet: "\\Flow\\ETL\\DSL\\datetime_schema(${1:string $name}, ${2:bool $nullable}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-schema",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">datetime_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span></code></pre><p>@return Definition<\DateTimeInterface></p>"

                },
                                {
                    name: "date_entry",
                    caption: "date_entry",
                    snippet: "\\Flow\\ETL\\DSL\\date_entry(${1:string $name}, ${2:DateTimeInterface|string|null $value}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-entries",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">date_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DateTimeInterface|string|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span></code></pre><p>@return Entry<?\DateTimeInterface></p>"

                },
                                {
                    name: "date_interval_to_microseconds",
                    caption: "date_interval_to_microseconds",
                    snippet: "\\Flow\\ETL\\DSL\\date_interval_to_microseconds(${1:DateInterval $interval})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">date_interval_to_microseconds</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DateInterval</span> <span class=\"fn-param\">$interval</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">int</span></code></pre>"

                },
                                {
                    name: "date_interval_to_milliseconds",
                    caption: "date_interval_to_milliseconds",
                    snippet: "\\Flow\\ETL\\DSL\\date_interval_to_milliseconds(${1:DateInterval $interval})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">date_interval_to_milliseconds</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DateInterval</span> <span class=\"fn-param\">$interval</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">int</span></code></pre>"

                },
                                {
                    name: "date_interval_to_seconds",
                    caption: "date_interval_to_seconds",
                    snippet: "\\Flow\\ETL\\DSL\\date_interval_to_seconds(${1:DateInterval $interval})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">date_interval_to_seconds</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DateInterval</span> <span class=\"fn-param\">$interval</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">int</span></code></pre>"

                },
                                {
                    name: "date_schema",
                    caption: "date_schema",
                    snippet: "\\Flow\\ETL\\DSL\\date_schema(${1:string $name}, ${2:bool $nullable}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-schema",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">date_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span></code></pre><p>@return Definition<\DateTimeInterface></p>"

                },
                                {
                    name: "date_time_format",
                    caption: "date_time_format",
                    snippet: "\\Flow\\ETL\\DSL\\date_time_format(${1:ScalarFunction $ref}, ${2:string $format})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">date_time_format</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$format</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DateTimeFormat</span></code></pre>"

                },
                                {
                    name: "dbal_dataframe_factory",
                    caption: "dbal_dataframe_factory",
                    snippet: "\\Flow\\ETL\\Adapter\\Doctrine\\dbal_dataframe_factory(${1:Connection|array $connection}, ${2:string $query}, ${3:QueryParameter $parameters})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">dbal_dataframe_factory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection|array</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">QueryParameter</span> <span class=\"fn-param\">$parameters</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalDataFrameFactory</span></code></pre><p>@param array<string, mixed>|Connection $connection<br>@param string $query<br>@param QueryParameter ...$parameters</p>"

                },
                                {
                    name: "dbal_from_queries",
                    caption: "dbal_from_queries",
                    snippet: "\\Flow\\ETL\\Adapter\\Doctrine\\dbal_from_queries(${1:Connection $connection}, ${2:string $query}, ${3:ParametersSet $parameters_set}, ${4:array $types})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">dbal_from_queries</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ParametersSet</span> <span class=\"fn-param\">$parameters_set</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$types</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalQueryExtractor</span></code></pre><p>@deprecated use from_dbal_queries() instead<br>@param null|ParametersSet $parameters_set - each one parameters array will be evaluated as new query<br>@param array<int|string, DbalArrayType|DbalParameterType|DbalType|int|string> $types</p>"

                },
                                {
                    name: "dbal_from_query",
                    caption: "dbal_from_query",
                    snippet: "\\Flow\\ETL\\Adapter\\Doctrine\\dbal_from_query(${1:Connection $connection}, ${2:string $query}, ${3:array $parameters}, ${4:array $types})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">dbal_from_query</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$parameters</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$types</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalQueryExtractor</span></code></pre><p>@deprecated use from_dbal_query() instead<br>@param array<string, mixed>|list<mixed> $parameters - @deprecated use DbalQueryExtractor::withParameters() instead<br>@param array<int<0, max>|string, DbalArrayType|DbalParameterType|DbalType|string> $types - @deprecated use DbalQueryExtractor::withTypes() instead</p>"

                },
                                {
                    name: "delay_exponential",
                    caption: "delay_exponential",
                    snippet: "\\Flow\\ETL\\DSL\\delay_exponential(${1:Duration $base}, ${2:int $multiplier}, ${3:Duration $max_delay})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">delay_exponential</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Duration</span> <span class=\"fn-param\">$base</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$multiplier</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Duration</span> <span class=\"fn-param\">$max_delay</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Exponential</span></code></pre>"

                },
                                {
                    name: "delay_fixed",
                    caption: "delay_fixed",
                    snippet: "\\Flow\\ETL\\DSL\\delay_fixed(${1:Duration $delay})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">delay_fixed</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Duration</span> <span class=\"fn-param\">$delay</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Fixed</span></code></pre>"

                },
                                {
                    name: "delay_jitter",
                    caption: "delay_jitter",
                    snippet: "\\Flow\\ETL\\DSL\\delay_jitter(${1:DelayFactory $delay}, ${2:float $jitter_factor})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">delay_jitter</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DelayFactory</span> <span class=\"fn-param\">$delay</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">float</span> <span class=\"fn-param\">$jitter_factor</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Jitter</span></code></pre><p>@param float $jitter_factor a value between 0 and 1 representing the maximum percentage of jitter to apply</p>"

                },
                                {
                    name: "delay_linear",
                    caption: "delay_linear",
                    snippet: "\\Flow\\ETL\\DSL\\delay_linear(${1:Duration $delay}, ${2:Duration $increment})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">delay_linear</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Duration</span> <span class=\"fn-param\">$delay</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Duration</span> <span class=\"fn-param\">$increment</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Linear</span></code></pre>"

                },
                                {
                    name: "dense_rank",
                    caption: "dense_rank",
                    snippet: "\\Flow\\ETL\\DSL\\dense_rank()",
                    meta: "flow-dsl-window-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">dense_rank</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DenseRank</span></code></pre>"

                },
                                {
                    name: "dens_rank",
                    caption: "dens_rank",
                    snippet: "\\Flow\\ETL\\DSL\\dens_rank()",
                    meta: "flow-dsl-window-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">dens_rank</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DenseRank</span></code></pre>"

                },
                                {
                    name: "df",
                    caption: "df",
                    snippet: "\\Flow\\ETL\\DSL\\df(${1:Config|ConfigBuilder|null $config})",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">df</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Config|ConfigBuilder|null</span> <span class=\"fn-param\">$config</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Flow</span></code></pre><p>Alias for data_frame() : Flow.</p>"

                },
                                {
                    name: "dom_element_to_string",
                    caption: "dom_element_to_string",
                    snippet: "\\Flow\\ETL\\DSL\\dom_element_to_string(${1:DOMElement $element}, ${2:bool $format_output}, ${3:bool $preserver_white_space})",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">dom_element_to_string</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DOMElement</span> <span class=\"fn-param\">$element</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$format_output</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$preserver_white_space</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string|false</span></code></pre><p>@deprecated Please use \Flow\Types\DSL\dom_element_to_string() instead</p>"

                },
                                {
                    name: "dom_element_to_string",
                    caption: "dom_element_to_string",
                    snippet: "\\Flow\\Types\\DSL\\dom_element_to_string(${1:DOMElement $element}, ${2:bool $format_output}, ${3:bool $preserver_white_space})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">dom_element_to_string</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DOMElement</span> <span class=\"fn-param\">$element</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$format_output</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$preserver_white_space</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string|false</span></code></pre>"

                },
                                {
                    name: "drop",
                    caption: "drop",
                    snippet: "\\Flow\\ETL\\DSL\\drop(${1:Reference|string $entries})",
                    meta: "flow-dsl-transformers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">drop</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$entries</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Drop</span></code></pre>"

                },
                                {
                    name: "duration_microseconds",
                    caption: "duration_microseconds",
                    snippet: "\\Flow\\ETL\\DSL\\duration_microseconds(${1:int $microseconds})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">duration_microseconds</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$microseconds</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Duration</span></code></pre>"

                },
                                {
                    name: "duration_milliseconds",
                    caption: "duration_milliseconds",
                    snippet: "\\Flow\\ETL\\DSL\\duration_milliseconds(${1:int $milliseconds})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">duration_milliseconds</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$milliseconds</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Duration</span></code></pre>"

                },
                                {
                    name: "duration_minutes",
                    caption: "duration_minutes",
                    snippet: "\\Flow\\ETL\\DSL\\duration_minutes(${1:int $minutes})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">duration_minutes</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$minutes</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Duration</span></code></pre>"

                },
                                {
                    name: "duration_seconds",
                    caption: "duration_seconds",
                    snippet: "\\Flow\\ETL\\DSL\\duration_seconds(${1:int $seconds})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">duration_seconds</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$seconds</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Duration</span></code></pre>"

                },
                                {
                    name: "empty_generator",
                    caption: "empty_generator",
                    snippet: "\\Flow\\ETL\\Adapter\\Parquet\\empty_generator()",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">empty_generator</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Generator</span></code></pre>"

                },
                                {
                    name: "entries",
                    caption: "entries",
                    snippet: "\\Flow\\ETL\\DSL\\entries(${1:Entry $entries})",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">entries</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Entry</span> <span class=\"fn-param\">$entries</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entries</span></code></pre><p>@param Entry<mixed> ...$entries</p>"

                },
                                {
                    name: "entry",
                    caption: "entry",
                    snippet: "\\Flow\\ETL\\DSL\\entry(${1:string $entry})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">EntryReference</span></code></pre><p>An alias for `ref`.</p>"

                },
                                {
                    name: "entry_id_factory",
                    caption: "entry_id_factory",
                    snippet: "\\Flow\\ETL\\Adapter\\Elasticsearch\\entry_id_factory(${1:string $entry_name})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">entry_id_factory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry_name</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IdFactory</span></code></pre>"

                },
                                {
                    name: "enum_entry",
                    caption: "enum_entry",
                    snippet: "\\Flow\\ETL\\DSL\\enum_entry(${1:string $name}, ${2:UnitEnum $enum}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-entries",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">enum_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">UnitEnum</span> <span class=\"fn-param\">$enum</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span></code></pre><p>@return Entry<?\UnitEnum></p>"

                },
                                {
                    name: "enum_schema",
                    caption: "enum_schema",
                    snippet: "\\Flow\\ETL\\DSL\\enum_schema(${1:string $name}, ${2:string $type}, ${3:bool $nullable}, ${4:Metadata $metadata})",
                    meta: "flow-dsl-schema",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">enum_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span></code></pre><p>@template T of \UnitEnum<br>@param class-string<T> $type<br>@return Definition<T></p>"

                },
                                {
                    name: "equal",
                    caption: "equal",
                    snippet: "\\Flow\\ETL\\DSL\\equal(${1:Reference|string $left}, ${2:Reference|string $right})",
                    meta: "flow-dsl-comparisons",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">equal</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$right</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Equal</span></code></pre>"

                },
                                {
                    name: "es_hits_to_rows",
                    caption: "es_hits_to_rows",
                    snippet: "\\Flow\\ETL\\Adapter\\Elasticsearch\\es_hits_to_rows(${1:DocumentDataSource $source})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">es_hits_to_rows</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DocumentDataSource</span> <span class=\"fn-param\">$source</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">HitsIntoRowsTransformer</span></code></pre><p>Transforms elasticsearch results into clear Flow Rows using [\'hits\'][\'hits\'][x][\'_source\'].<br>@return HitsIntoRowsTransformer</p>"

                },
                                {
                    name: "exception_if_exists",
                    caption: "exception_if_exists",
                    snippet: "\\Flow\\ETL\\DSL\\exception_if_exists()",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">exception_if_exists</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SaveMode</span></code></pre>"

                },
                                {
                    name: "execution_context",
                    caption: "execution_context",
                    snippet: "\\Flow\\ETL\\DSL\\execution_context(${1:Config $config})",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">execution_context</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Config</span> <span class=\"fn-param\">$config</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FlowContext</span></code></pre>"

                },
                                {
                    name: "exists",
                    caption: "exists",
                    snippet: "\\Flow\\ETL\\DSL\\exists(${1:ScalarFunction $ref})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">exists</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Exists</span></code></pre>"

                },
                                {
                    name: "files",
                    caption: "files",
                    snippet: "\\Flow\\ETL\\DSL\\files(${1:Path|string $directory})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">files</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$directory</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FilesExtractor</span></code></pre>"

                },
                                {
                    name: "filesystem_cache",
                    caption: "filesystem_cache",
                    snippet: "\\Flow\\ETL\\DSL\\filesystem_cache(${1:Path|string|null $cache_dir}, ${2:Filesystem $filesystem}, ${3:Serializer $serializer})",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">filesystem_cache</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string|null</span> <span class=\"fn-param\">$cache_dir</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Filesystem</span> <span class=\"fn-param\">$filesystem</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Serializer</span> <span class=\"fn-param\">$serializer</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FilesystemCache</span></code></pre>"

                },
                                {
                    name: "first",
                    caption: "first",
                    snippet: "\\Flow\\ETL\\DSL\\first(${1:EntryReference|string $ref})",
                    meta: "flow-dsl-aggregating-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">first</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">First</span></code></pre>"

                },
                                {
                    name: "float_entry",
                    caption: "float_entry",
                    snippet: "\\Flow\\ETL\\DSL\\float_entry(${1:string $name}, ${2:string|int|float|null $value}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-entries",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">float_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string|int|float|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span></code></pre><p>@return Entry<?float></p>"

                },
                                {
                    name: "float_schema",
                    caption: "float_schema",
                    snippet: "\\Flow\\ETL\\DSL\\float_schema(${1:string $name}, ${2:bool $nullable}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-schema",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">float_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span></code></pre><p>@return Definition<float></p>"

                },
                                {
                    name: "flow_context",
                    caption: "flow_context",
                    snippet: "\\Flow\\ETL\\DSL\\flow_context(${1:Config $config})",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">flow_context</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Config</span> <span class=\"fn-param\">$config</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FlowContext</span></code></pre>"

                },
                                {
                    name: "from_all",
                    caption: "from_all",
                    snippet: "\\Flow\\ETL\\DSL\\from_all(${1:Extractor $extractors})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_all</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Extractor</span> <span class=\"fn-param\">$extractors</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ChainExtractor</span></code></pre>"

                },
                                {
                    name: "from_array",
                    caption: "from_array",
                    snippet: "\\Flow\\ETL\\DSL\\from_array(${1:iterable $array}, ${2:Schema $schema})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_array</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">iterable</span> <span class=\"fn-param\">$array</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayExtractor</span></code></pre><p>@param iterable<array<mixed>> $array<br>@param null|Schema $schema - @deprecated use withSchema() method instead</p>"

                },
                                {
                    name: "from_avro",
                    caption: "from_avro",
                    snippet: "\\Flow\\ETL\\DSL\\Adapter\\Avro\\from_avro(${1:Path|string $path})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_avro</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AvroExtractor</span></code></pre>"

                },
                                {
                    name: "from_cache",
                    caption: "from_cache",
                    snippet: "\\Flow\\ETL\\DSL\\from_cache(${1:string $id}, ${2:Extractor $fallback_extractor}, ${3:bool $clear})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_cache</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$id</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Extractor</span> <span class=\"fn-param\">$fallback_extractor</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$clear</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CacheExtractor</span></code></pre><p>@param string $id - cache id from which data will be extracted<br>@param null|Extractor $fallback_extractor - extractor that will be used when cache is empty - @deprecated use withFallbackExtractor() method instead<br>@param bool $clear - clear cache after extraction - @deprecated use withClearOnFinish() method instead</p>"

                },
                                {
                    name: "from_csv",
                    caption: "from_csv",
                    snippet: "\\Flow\\ETL\\Adapter\\CSV\\from_csv(${1:Path|string $path}, ${2:bool $with_header}, ${3:bool $empty_to_null}, ${4:string $separator}, ${5:string $enclosure}, ${6:string $escape}, ${7:int $characters_read_in_line}, ${8:Schema $schema})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_csv</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$with_header</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$empty_to_null</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$separator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$enclosure</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$escape</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$characters_read_in_line</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CSVExtractor</span></code></pre><p>@param Path|string $path<br>@param bool $empty_to_null - @deprecated use $loader->withEmptyToNull() instead<br>@param bool $with_header - @deprecated use $loader->withHeader() instead<br>@param null|string $separator - @deprecated use $loader->withSeparator() instead<br>@param null|string $enclosure - @deprecated use $loader->withEnclosure() instead<br>@param null|string $escape - @deprecated use $loader->withEscape() instead<br>@param int<1, max> $characters_read_in_line - @deprecated use $loader->withCharactersReadInLine() instead<br>@param null|Schema $schema - @deprecated use $loader->withSchema() instead</p>"

                },
                                {
                    name: "from_data_frame",
                    caption: "from_data_frame",
                    snippet: "\\Flow\\ETL\\DSL\\from_data_frame(${1:DataFrame $data_frame})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_data_frame</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DataFrame</span> <span class=\"fn-param\">$data_frame</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataFrameExtractor</span></code></pre>"

                },
                                {
                    name: "from_dbal_key_set_qb",
                    caption: "from_dbal_key_set_qb",
                    snippet: "\\Flow\\ETL\\Adapter\\Doctrine\\from_dbal_key_set_qb(${1:Connection $connection}, ${2:QueryBuilder $queryBuilder}, ${3:KeySet $key_set})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_dbal_key_set_qb</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">QueryBuilder</span> <span class=\"fn-param\">$queryBuilder</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">KeySet</span> <span class=\"fn-param\">$key_set</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalKeySetExtractor</span></code></pre>"

                },
                                {
                    name: "from_dbal_limit_offset",
                    caption: "from_dbal_limit_offset",
                    snippet: "\\Flow\\ETL\\Adapter\\Doctrine\\from_dbal_limit_offset(${1:Connection $connection}, ${2:Table|string $table}, ${3:OrderBy|array $order_by}, ${4:int $page_size}, ${5:int $maximum})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_dbal_limit_offset</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Table|string</span> <span class=\"fn-param\">$table</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">OrderBy|array</span> <span class=\"fn-param\">$order_by</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$page_size</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$maximum</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalLimitOffsetExtractor</span></code></pre><p>@param Connection $connection<br>@param string|Table $table<br>@param array<OrderBy>|OrderBy $order_by<br>@param int $page_size<br>@param null|int $maximum<br>@throws InvalidArgumentException</p>"

                },
                                {
                    name: "from_dbal_limit_offset_qb",
                    caption: "from_dbal_limit_offset_qb",
                    snippet: "\\Flow\\ETL\\Adapter\\Doctrine\\from_dbal_limit_offset_qb(${1:Connection $connection}, ${2:QueryBuilder $queryBuilder}, ${3:int $page_size}, ${4:int $maximum}, ${5:int $offset})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_dbal_limit_offset_qb</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">QueryBuilder</span> <span class=\"fn-param\">$queryBuilder</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$page_size</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$maximum</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalLimitOffsetExtractor</span></code></pre><p>@param Connection $connection<br>@param int $page_size<br>@param null|int $maximum - maximum can also be taken from a query builder, $maximum however is used regardless of the query builder if it\'s set<br>@param int $offset - offset can also be taken from a query builder, $offset however is used regardless of the query builder if it\'s set to non 0 value</p>"

                },
                                {
                    name: "from_dbal_queries",
                    caption: "from_dbal_queries",
                    snippet: "\\Flow\\ETL\\Adapter\\Doctrine\\from_dbal_queries(${1:Connection $connection}, ${2:string $query}, ${3:ParametersSet $parameters_set}, ${4:array $types})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_dbal_queries</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ParametersSet</span> <span class=\"fn-param\">$parameters_set</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$types</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalQueryExtractor</span></code></pre><p>@param null|ParametersSet $parameters_set - each one parameters array will be evaluated as new query<br>@param array<int|string, DbalArrayType|DbalParameterType|DbalType|int|string> $types</p>"

                },
                                {
                    name: "from_dbal_query",
                    caption: "from_dbal_query",
                    snippet: "\\Flow\\ETL\\Adapter\\Doctrine\\from_dbal_query(${1:Connection $connection}, ${2:string $query}, ${3:array $parameters}, ${4:array $types})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_dbal_query</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$parameters</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$types</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalQueryExtractor</span></code></pre><p>@param array<string, mixed>|list<mixed> $parameters - @deprecated use DbalQueryExtractor::withParameters() instead<br>@param array<int<0, max>|string, DbalArrayType|DbalParameterType|DbalType|string> $types - @deprecated use DbalQueryExtractor::withTypes() instead</p>"

                },
                                {
                    name: "from_dynamic_http_requests",
                    caption: "from_dynamic_http_requests",
                    snippet: "\\Flow\\ETL\\Adapter\\Http\\from_dynamic_http_requests(${1:ClientInterface $client}, ${2:NextRequestFactory $requestFactory})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_dynamic_http_requests</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ClientInterface</span> <span class=\"fn-param\">$client</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">NextRequestFactory</span> <span class=\"fn-param\">$requestFactory</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PsrHttpClientDynamicExtractor</span></code></pre>"

                },
                                {
                    name: "from_es",
                    caption: "from_es",
                    snippet: "\\Flow\\ETL\\Adapter\\Elasticsearch\\from_es(${1:array $config}, ${2:array $parameters}, ${3:array $pit_params})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_es</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$config</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$parameters</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$pit_params</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ElasticsearchExtractor</span></code></pre><p>Extractor will automatically try to iterate over whole index using one of the two iteration methods:.<br>- from/size<br>- search_after<br>Search after is selected when you provide define sort parameters in query, otherwise it will fallback to from/size.<br>@param array{<br> hosts?: array<string>,<br> connectionParams?: array<mixed>,<br> retries?: int,<br> sniffOnStart?: bool,<br> sslCert?: array<string>,<br> sslKey?: array<string>,<br> sslVerification?: bool|string,<br> elasticMetaHeader?: bool,<br> includePortInHostHeader?: bool<br>} $config<br>@param array<mixed> $parameters - https://www.elastic.co/guide/en/elasticsearch/reference/master/search-search.html<br>@param ?array<mixed> $pit_params - when used extractor will create point in time to stabilize search results. Point in time is automatically closed when last element is extracted. https://www.elastic.co/guide/en/elasticsearch/reference/master/point-in-time-api.html - @deprecated use withPointInTime method instead</p>"

                },
                                {
                    name: "from_excel",
                    caption: "from_excel",
                    snippet: "\\Flow\\ETL\\Adapter\\Excel\\DSL\\from_excel(${1:Path|string $path})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_excel</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ExcelExtractor</span></code></pre>"

                },
                                {
                    name: "from_google_sheet",
                    caption: "from_google_sheet",
                    snippet: "\\Flow\\ETL\\Adapter\\GoogleSheet\\from_google_sheet(${1:Sheets|array $auth_config}, ${2:string $spreadsheet_id}, ${3:string $sheet_name}, ${4:bool $with_header}, ${5:int $rows_per_page}, ${6:array $options})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_google_sheet</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Sheets|array</span> <span class=\"fn-param\">$auth_config</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$spreadsheet_id</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$sheet_name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$with_header</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$rows_per_page</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">GoogleSheetExtractor</span></code></pre><p>@param array{type: string, project_id: string, private_key_id: string, private_key: string, client_email: string, client_id: string, auth_uri: string, token_uri: string, auth_provider_x509_cert_url: string, client_x509_cert_url: string}|Sheets $auth_config<br>@param string $spreadsheet_id<br>@param string $sheet_name<br>@param bool $with_header - @deprecated use withHeader method instead<br>@param int $rows_per_page - how many rows per page to fetch from Google Sheets API - @deprecated use withRowsPerPage method instead<br>@param array{dateTimeRenderOption?: string, majorDimension?: string, valueRenderOption?: string} $options - @deprecated use withOptions method instead</p>"

                },
                                {
                    name: "from_google_sheet_columns",
                    caption: "from_google_sheet_columns",
                    snippet: "\\Flow\\ETL\\Adapter\\GoogleSheet\\from_google_sheet_columns(${1:Sheets|array $auth_config}, ${2:string $spreadsheet_id}, ${3:string $sheet_name}, ${4:string $start_range_column}, ${5:string $end_range_column}, ${6:bool $with_header}, ${7:int $rows_per_page}, ${8:array $options})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_google_sheet_columns</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Sheets|array</span> <span class=\"fn-param\">$auth_config</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$spreadsheet_id</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$sheet_name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$start_range_column</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$end_range_column</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$with_header</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$rows_per_page</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">GoogleSheetExtractor</span></code></pre><p>@param array{type: string, project_id: string, private_key_id: string, private_key: string, client_email: string, client_id: string, auth_uri: string, token_uri: string, auth_provider_x509_cert_url: string, client_x509_cert_url: string}|Sheets $auth_config<br>@param string $spreadsheet_id<br>@param string $sheet_name<br>@param string $start_range_column<br>@param string $end_range_column<br>@param bool $with_header - @deprecated use withHeader method instead<br>@param int $rows_per_page - how many rows per page to fetch from Google Sheets API, default 1000 - @deprecated use withRowsPerPage method instead<br>@param array{dateTimeRenderOption?: string, majorDimension?: string, valueRenderOption?: string} $options - @deprecated use withOptions method instead</p>"

                },
                                {
                    name: "from_json",
                    caption: "from_json",
                    snippet: "\\Flow\\ETL\\Adapter\\JSON\\from_json(${1:Path|string $path}, ${2:string $pointer}, ${3:Schema $schema})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_json</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$pointer</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">JsonExtractor</span></code></pre><p>@param Path|string $path - string is internally turned into stream<br>@param ?string $pointer - if you want to iterate only results of a subtree, use a pointer, read more at https://github.com/halaxa/json-machine#parsing-a-subtree - @deprecate use withPointer method instead<br>@param null|Schema $schema - enforce schema on the extracted data - @deprecate use withSchema method instead</p>"

                },
                                {
                    name: "from_json_lines",
                    caption: "from_json_lines",
                    snippet: "\\Flow\\ETL\\Adapter\\JSON\\from_json_lines(${1:Path|string $path})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_json_lines</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">JsonLinesExtractor</span></code></pre><p>Used to read from a JSON lines https://jsonlines.org/ formatted file.<br>@param Path|string $path - string is internally turned into stream</p>"

                },
                                {
                    name: "from_meilisearch",
                    caption: "from_meilisearch",
                    snippet: "\\Flow\\ETL\\Adapter\\Meilisearch\\from_meilisearch(${1:array $config}, ${2:array $params}, ${3:string $index})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_meilisearch</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$config</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$params</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$index</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MeilisearchExtractor</span></code></pre><p>@param array{url: string, apiKey: string} $config<br>@param array{q: string, limit?: ?int, offset?: ?int, attributesToRetrieve?: ?array<string>, sort?: ?array<string>} $params</p>"

                },
                                {
                    name: "from_memory",
                    caption: "from_memory",
                    snippet: "\\Flow\\ETL\\DSL\\from_memory(${1:Memory $memory})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_memory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Memory</span> <span class=\"fn-param\">$memory</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MemoryExtractor</span></code></pre>"

                },
                                {
                    name: "from_parquet",
                    caption: "from_parquet",
                    snippet: "\\Flow\\ETL\\Adapter\\Parquet\\from_parquet(${1:Path|string $path}, ${2:array $columns}, ${3:Options $options}, ${4:ByteOrder $byte_order}, ${5:int $offset})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_parquet</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Options</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ByteOrder</span> <span class=\"fn-param\">$byte_order</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ParquetExtractor</span></code></pre><p>@param Path|string $path<br>@param array<string> $columns - list of columns to read from parquet file - @deprecated use `withColumns` method instead<br>@param Options $options - @deprecated use `withOptions` method instead<br>@param ByteOrder $byte_order - @deprecated use `withByteOrder` method instead<br>@param null|int $offset - @deprecated use `withOffset` method instead</p>"

                },
                                {
                    name: "from_path_partitions",
                    caption: "from_path_partitions",
                    snippet: "\\Flow\\ETL\\DSL\\from_path_partitions(${1:Path|string $path})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_path_partitions</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PathPartitionsExtractor</span></code></pre>"

                },
                                {
                    name: "from_pipeline",
                    caption: "from_pipeline",
                    snippet: "\\Flow\\ETL\\DSL\\from_pipeline(${1:Pipeline $pipeline})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_pipeline</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Pipeline</span> <span class=\"fn-param\">$pipeline</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PipelineExtractor</span></code></pre>"

                },
                                {
                    name: "from_rows",
                    caption: "from_rows",
                    snippet: "\\Flow\\ETL\\DSL\\from_rows(${1:Rows $rows})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_rows</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Rows</span> <span class=\"fn-param\">$rows</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RowsExtractor</span></code></pre>"

                },
                                {
                    name: "from_sequence_date_period",
                    caption: "from_sequence_date_period",
                    snippet: "\\Flow\\ETL\\DSL\\from_sequence_date_period(${1:string $entry_name}, ${2:DateTimeInterface $start}, ${3:DateInterval $interval}, ${4:DateTimeInterface $end}, ${5:int $options})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_sequence_date_period</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry_name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DateTimeInterface</span> <span class=\"fn-param\">$start</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DateInterval</span> <span class=\"fn-param\">$interval</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DateTimeInterface</span> <span class=\"fn-param\">$end</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SequenceExtractor</span></code></pre>"

                },
                                {
                    name: "from_sequence_date_period_recurrences",
                    caption: "from_sequence_date_period_recurrences",
                    snippet: "\\Flow\\ETL\\DSL\\from_sequence_date_period_recurrences(${1:string $entry_name}, ${2:DateTimeInterface $start}, ${3:DateInterval $interval}, ${4:int $recurrences}, ${5:int $options})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_sequence_date_period_recurrences</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry_name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DateTimeInterface</span> <span class=\"fn-param\">$start</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DateInterval</span> <span class=\"fn-param\">$interval</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$recurrences</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SequenceExtractor</span></code></pre>"

                },
                                {
                    name: "from_sequence_number",
                    caption: "from_sequence_number",
                    snippet: "\\Flow\\ETL\\DSL\\from_sequence_number(${1:string $entry_name}, ${2:string|int|float $start}, ${3:string|int|float $end}, ${4:int|float $step})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_sequence_number</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry_name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string|int|float</span> <span class=\"fn-param\">$start</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string|int|float</span> <span class=\"fn-param\">$end</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int|float</span> <span class=\"fn-param\">$step</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SequenceExtractor</span></code></pre>"

                },
                                {
                    name: "from_static_http_requests",
                    caption: "from_static_http_requests",
                    snippet: "\\Flow\\ETL\\Adapter\\Http\\from_static_http_requests(${1:ClientInterface $client}, ${2:iterable $requests})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_static_http_requests</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ClientInterface</span> <span class=\"fn-param\">$client</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">iterable</span> <span class=\"fn-param\">$requests</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PsrHttpClientStaticExtractor</span></code></pre><p>@param iterable<RequestInterface> $requests</p>"

                },
                                {
                    name: "from_text",
                    caption: "from_text",
                    snippet: "\\Flow\\ETL\\Adapter\\Text\\from_text(${1:Path|string $path})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_text</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">TextExtractor</span></code></pre><p>@param Path|string $path</p>"

                },
                                {
                    name: "from_xml",
                    caption: "from_xml",
                    snippet: "\\Flow\\ETL\\Adapter\\XML\\from_xml(${1:Path|string $path}, ${2:string $xml_node_path})",
                    meta: "flow-dsl-extractors",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">from_xml</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$xml_node_path</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">XMLParserExtractor</span></code></pre><p> In order to iterate only over <element> nodes use `from_xml($file)->withXMLNodePath(\'root/elements/element\')`.<br> <root><br>   <elements><br>     <element></element><br>     <element></element><br>   <elements><br> </root><br> XML Node Path does not support attributes and it\'s not xpath, it is just a sequence<br> of node names separated with slash.<br>@param Path|string $path<br>@param string $xml_node_path - @deprecated use `from_xml($file)->withXMLNodePath($xmlNodePath)` method instead</p>"

                },
                                {
                    name: "fstab",
                    caption: "fstab",
                    snippet: "\\Flow\\Filesystem\\DSL\\fstab(${1:Filesystem $filesystems})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">fstab</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Filesystem</span> <span class=\"fn-param\">$filesystems</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FilesystemTable</span></code></pre><p>Create a new filesystem table with given filesystems.<br>Filesystems can be also mounted later.<br>If no filesystems are provided, local filesystem is mounted.</p>"

                },
                                {
                    name: "generate_random_int",
                    caption: "generate_random_int",
                    snippet: "\\Flow\\ETL\\DSL\\generate_random_int(${1:int $start}, ${2:int $end}, ${3:NativePHPRandomValueGenerator $generator})",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">generate_random_int</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$start</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$end</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">NativePHPRandomValueGenerator</span> <span class=\"fn-param\">$generator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">int</span></code></pre>"

                },
                                {
                    name: "generate_random_string",
                    caption: "generate_random_string",
                    snippet: "\\Flow\\ETL\\DSL\\generate_random_string(${1:int $length}, ${2:NativePHPRandomValueGenerator $generator})",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">generate_random_string</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$length</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">NativePHPRandomValueGenerator</span> <span class=\"fn-param\">$generator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span></code></pre>"

                },
                                {
                    name: "get_type",
                    caption: "get_type",
                    snippet: "\\Flow\\ETL\\DSL\\get_type(${1:mixed $value})",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">get_type</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@return Type<mixed><br>@deprecated Please use \Flow\Types\DSL\get_type($value) instead</p>"

                },
                                {
                    name: "get_type",
                    caption: "get_type",
                    snippet: "\\Flow\\Types\\DSL\\get_type(${1:mixed $value})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">get_type</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@return Type<mixed></p>"

                },
                                {
                    name: "greatest",
                    caption: "greatest",
                    snippet: "\\Flow\\ETL\\DSL\\greatest(${1:mixed $values})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">greatest</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$values</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Greatest</span></code></pre>"

                },
                                {
                    name: "hash",
                    caption: "hash",
                    snippet: "\\Flow\\ETL\\DSL\\hash(${1:mixed $value}, ${2:Algorithm $algorithm})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">hash</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Algorithm</span> <span class=\"fn-param\">$algorithm</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Hash</span></code></pre>"

                },
                                {
                    name: "hash_id_factory",
                    caption: "hash_id_factory",
                    snippet: "\\Flow\\ETL\\Adapter\\Elasticsearch\\hash_id_factory(${1:string $entry_names})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">hash_id_factory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry_names</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IdFactory</span></code></pre>"

                },
                                {
                    name: "html_entry",
                    caption: "html_entry",
                    snippet: "\\Flow\\ETL\\DSL\\html_entry(${1:string $name}, ${2:HTMLDocument|string|null $value}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-entries",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">html_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">HTMLDocument|string|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span></code></pre><p>@return Entry<?HTMLDocument></p>"

                },
                                {
                    name: "html_schema",
                    caption: "html_schema",
                    snippet: "\\Flow\\ETL\\DSL\\html_schema(${1:string $name}, ${2:bool $nullable}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-schema",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">html_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span></code></pre><p>@return Definition<HTMLDocument></p>"

                },
                                {
                    name: "identical",
                    caption: "identical",
                    snippet: "\\Flow\\ETL\\DSL\\identical(${1:Reference|string $left}, ${2:Reference|string $right})",
                    meta: "flow-dsl-comparisons",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">identical</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$right</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Identical</span></code></pre>"

                },
                                {
                    name: "ignore",
                    caption: "ignore",
                    snippet: "\\Flow\\ETL\\DSL\\ignore()",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">ignore</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SaveMode</span></code></pre>"

                },
                                {
                    name: "ignore_error_handler",
                    caption: "ignore_error_handler",
                    snippet: "\\Flow\\ETL\\DSL\\ignore_error_handler()",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">ignore_error_handler</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IgnoreError</span></code></pre>"

                },
                                {
                    name: "integer_entry",
                    caption: "integer_entry",
                    snippet: "\\Flow\\ETL\\DSL\\integer_entry(${1:string $name}, ${2:int $value}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-entries",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">integer_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span></code></pre><p>@return Entry<?int></p>"

                },
                                {
                    name: "integer_schema",
                    caption: "integer_schema",
                    snippet: "\\Flow\\ETL\\DSL\\integer_schema(${1:string $name}, ${2:bool $nullable}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-schema",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">integer_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span></code></pre><p>@return Definition<int></p>"

                },
                                {
                    name: "int_entry",
                    caption: "int_entry",
                    snippet: "\\Flow\\ETL\\DSL\\int_entry(${1:string $name}, ${2:int $value}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-entries",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">int_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span></code></pre><p>@return Entry<?int></p>"

                },
                                {
                    name: "int_schema",
                    caption: "int_schema",
                    snippet: "\\Flow\\ETL\\DSL\\int_schema(${1:string $name}, ${2:bool $nullable}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-schema",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">int_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span></code></pre><p>Alias for `int_schema`.<br>@return Definition<int></p>"

                },
                                {
                    name: "is_type",
                    caption: "is_type",
                    snippet: "\\Flow\\ETL\\DSL\\is_type(${1:Type|array $type}, ${2:mixed $value})",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">is_type</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type|array</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">bool</span></code></pre><p>@param array<string|Type<mixed>>|Type<mixed> $type<br>@param mixed $value</p>"

                },
                                {
                    name: "is_valid_excel_sheet_name",
                    caption: "is_valid_excel_sheet_name",
                    snippet: "\\Flow\\ETL\\Adapter\\Excel\\DSL\\is_valid_excel_sheet_name(${1:ScalarFunction|string $sheet_name})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">is_valid_excel_sheet_name</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$sheet_name</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IsValidExcelSheetName</span></code></pre>"

                },
                                {
                    name: "join_on",
                    caption: "join_on",
                    snippet: "\\Flow\\ETL\\DSL\\join_on(${1:Comparison|array $comparisons}, ${2:string $join_prefix})",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">join_on</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Comparison|array</span> <span class=\"fn-param\">$comparisons</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$join_prefix</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Expression</span></code></pre><p>@param array<\Flow\ETL\Join\Comparison|string>|Comparison $comparisons</p>"

                },
                                {
                    name: "json_entry",
                    caption: "json_entry",
                    snippet: "\\Flow\\ETL\\DSL\\json_entry(${1:string $name}, ${2:array|string|null $data}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-entries",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">json_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array|string|null</span> <span class=\"fn-param\">$data</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span></code></pre><p>@param null|array<array-key, mixed>|string $data<br>@return Entry<?array<mixed>></p>"

                },
                                {
                    name: "json_object_entry",
                    caption: "json_object_entry",
                    snippet: "\\Flow\\ETL\\DSL\\json_object_entry(${1:string $name}, ${2:array|string|null $data}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-entries",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">json_object_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array|string|null</span> <span class=\"fn-param\">$data</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span></code></pre><p>@param null|array<array-key, mixed>|string $data<br>@throws InvalidArgumentException<br>@return Entry<mixed></p>"

                },
                                {
                    name: "json_schema",
                    caption: "json_schema",
                    snippet: "\\Flow\\ETL\\DSL\\json_schema(${1:string $name}, ${2:bool $nullable}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-schema",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">json_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span></code></pre><p>@return Definition<string></p>"

                },
                                {
                    name: "last",
                    caption: "last",
                    snippet: "\\Flow\\ETL\\DSL\\last(${1:EntryReference|string $ref})",
                    meta: "flow-dsl-aggregating-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">last</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Last</span></code></pre>"

                },
                                {
                    name: "least",
                    caption: "least",
                    snippet: "\\Flow\\ETL\\DSL\\least(${1:mixed $values})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">least</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$values</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Least</span></code></pre>"

                },
                                {
                    name: "limit",
                    caption: "limit",
                    snippet: "\\Flow\\ETL\\DSL\\limit(${1:int $limit})",
                    meta: "flow-dsl-transformers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">limit</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$limit</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Limit</span></code></pre>"

                },
                                {
                    name: "line_chart",
                    caption: "line_chart",
                    snippet: "\\Flow\\ETL\\Adapter\\ChartJS\\line_chart(${1:EntryReference $label}, ${2:References $datasets})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">line_chart</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference</span> <span class=\"fn-param\">$label</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">References</span> <span class=\"fn-param\">$datasets</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">LineChart</span></code></pre>"

                },
                                {
                    name: "list_entry",
                    caption: "list_entry",
                    snippet: "\\Flow\\ETL\\DSL\\list_entry(${1:string $name}, ${2:array $value}, ${3:ListType $type}, ${4:Metadata $metadata})",
                    meta: "flow-dsl-entries",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">list_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ListType</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span></code></pre><p>@template T<br>@param null|list<mixed> $value<br>@param ListType<T> $type<br>@return Entry<mixed></p>"

                },
                                {
                    name: "list_ref",
                    caption: "list_ref",
                    snippet: "\\Flow\\ETL\\DSL\\list_ref(${1:string $entry})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">list_ref</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ListFunctions</span></code></pre>"

                },
                                {
                    name: "list_schema",
                    caption: "list_schema",
                    snippet: "\\Flow\\ETL\\DSL\\list_schema(${1:string $name}, ${2:Type $type}, ${3:bool $nullable}, ${4:Metadata $metadata})",
                    meta: "flow-dsl-schema",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">list_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span></code></pre><p>@template T<br>@param Type<list<T>> $type<br>@return Definition<list<T>></p>"

                },
                                {
                    name: "lit",
                    caption: "lit",
                    snippet: "\\Flow\\ETL\\DSL\\lit(${1:mixed $value})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">lit</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Literal</span></code></pre>"

                },
                                {
                    name: "lower",
                    caption: "lower",
                    snippet: "\\Flow\\ETL\\DSL\\lower(${1:ScalarFunction|string $value})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">lower</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ToLower</span></code></pre>"

                },
                                {
                    name: "map_entry",
                    caption: "map_entry",
                    snippet: "\\Flow\\ETL\\DSL\\map_entry(${1:string $name}, ${2:array $value}, ${3:Type $mapType}, ${4:Metadata $metadata})",
                    meta: "flow-dsl-entries",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">map_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$mapType</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span></code></pre><p>@template TKey of array-key<br>@template TValue<br>@param ?array<array-key, mixed> $value<br>@param Type<array<TKey, TValue>> $mapType<br>@return Entry<?array<TKey, TValue>></p>"

                },
                                {
                    name: "map_schema",
                    caption: "map_schema",
                    snippet: "\\Flow\\ETL\\DSL\\map_schema(${1:string $name}, ${2:Type $type}, ${3:bool $nullable}, ${4:Metadata $metadata})",
                    meta: "flow-dsl-schema",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">map_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span></code></pre><p>@template TKey of array-key<br>@template TValue<br>@param Type<array<TKey, TValue>> $type<br>@return Definition<array<TKey, TValue>></p>"

                },
                                {
                    name: "mask_columns",
                    caption: "mask_columns",
                    snippet: "\\Flow\\ETL\\DSL\\mask_columns(${1:array $columns}, ${2:string $mask})",
                    meta: "flow-dsl-transformers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">mask_columns</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$mask</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MaskColumns</span></code></pre><p>@param array<int, string> $columns</p>"

                },
                                {
                    name: "match_cases",
                    caption: "match_cases",
                    snippet: "\\Flow\\ETL\\DSL\\match_cases(${1:array $cases}, ${2:mixed $default})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">match_cases</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$cases</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$default</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MatchCases</span></code></pre><p>@param array<MatchCondition> $cases</p>"

                },
                                {
                    name: "match_condition",
                    caption: "match_condition",
                    snippet: "\\Flow\\ETL\\DSL\\match_condition(${1:mixed $condition}, ${2:mixed $then})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">match_condition</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$condition</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$then</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MatchCondition</span></code></pre>"

                },
                                {
                    name: "max",
                    caption: "max",
                    snippet: "\\Flow\\ETL\\DSL\\max(${1:EntryReference|string $ref})",
                    meta: "flow-dsl-aggregating-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">max</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Max</span></code></pre>"

                },
                                {
                    name: "meilisearch_hits_to_rows",
                    caption: "meilisearch_hits_to_rows",
                    snippet: "\\Flow\\ETL\\Adapter\\Meilisearch\\meilisearch_hits_to_rows()",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">meilisearch_hits_to_rows</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">HitsIntoRowsTransformer</span></code></pre><p>Transforms Meilisearch results into clear Flow Rows.</p>"

                },
                                {
                    name: "memory_filesystem",
                    caption: "memory_filesystem",
                    snippet: "\\Flow\\Filesystem\\DSL\\memory_filesystem()",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">memory_filesystem</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MemoryFilesystem</span></code></pre><p>Create a new memory filesystem and writes data to it in memory.</p>"

                },
                                {
                    name: "min",
                    caption: "min",
                    snippet: "\\Flow\\ETL\\DSL\\min(${1:EntryReference|string $ref})",
                    meta: "flow-dsl-aggregating-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">min</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Min</span></code></pre>"

                },
                                {
                    name: "mysql_insert_options",
                    caption: "mysql_insert_options",
                    snippet: "\\Flow\\ETL\\Adapter\\Doctrine\\mysql_insert_options(${1:bool $skip_conflicts}, ${2:bool $upsert}, ${3:array $update_columns})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">mysql_insert_options</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">bool</span> <span class=\"fn-param\">$skip_conflicts</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$upsert</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$update_columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MySQLInsertOptions</span></code></pre><p>@param array<string> $update_columns</p>"

                },
                                {
                    name: "native_local_filesystem",
                    caption: "native_local_filesystem",
                    snippet: "\\Flow\\Filesystem\\DSL\\native_local_filesystem()",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">native_local_filesystem</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">NativeLocalFilesystem</span></code></pre>"

                },
                                {
                    name: "not",
                    caption: "not",
                    snippet: "\\Flow\\ETL\\DSL\\not(${1:ScalarFunction $value})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">not</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Not</span></code></pre>"

                },
                                {
                    name: "now",
                    caption: "now",
                    snippet: "\\Flow\\ETL\\DSL\\now(${1:DateTimeZone|ScalarFunction $time_zone})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">now</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DateTimeZone|ScalarFunction</span> <span class=\"fn-param\">$time_zone</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Now</span></code></pre>"

                },
                                {
                    name: "null_entry",
                    caption: "null_entry",
                    snippet: "\\Flow\\ETL\\DSL\\null_entry(${1:string $name}, ${2:Metadata $metadata})",
                    meta: "flow-dsl-entries",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">null_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span></code></pre><p>This functions is an alias for creating string entry from null.<br>The main difference between using this function an simply str_entry with second argument null<br>is that this function will also keep a note in the metadata that type might not be final.<br>For example when we need to guess column type from rows because schema was not provided,<br>and given column in the first row is null, it might still change once we get to the second row.<br>That metadata is used to determine if string_entry was created from null or not.<br>By design flow assumes when guessing column type that null would be a string (the most flexible type).<br>@return Entry<?string></p>"

                },
                                {
                    name: "null_schema",
                    caption: "null_schema",
                    snippet: "\\Flow\\ETL\\DSL\\null_schema(${1:string $name}, ${2:Metadata $metadata})",
                    meta: "flow-dsl-schema",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">null_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span></code></pre><p>@return Definition<string></p>"

                },
                                {
                    name: "number_format",
                    caption: "number_format",
                    snippet: "\\Flow\\ETL\\DSL\\number_format(${1:ScalarFunction|int|float $value}, ${2:ScalarFunction|int $decimals}, ${3:ScalarFunction|string $decimal_separator}, ${4:ScalarFunction|string $thousands_separator})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">number_format</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|int|float</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$decimals</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$decimal_separator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$thousands_separator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">NumberFormat</span></code></pre>"

                },
                                {
                    name: "optional",
                    caption: "optional",
                    snippet: "\\Flow\\ETL\\DSL\\optional(${1:ScalarFunction $function})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">optional</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$function</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Optional</span></code></pre>"

                },
                                {
                    name: "overwrite",
                    caption: "overwrite",
                    snippet: "\\Flow\\ETL\\DSL\\overwrite()",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">overwrite</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SaveMode</span></code></pre>"

                },
                                {
                    name: "pagination_key_asc",
                    caption: "pagination_key_asc",
                    snippet: "\\Flow\\ETL\\Adapter\\Doctrine\\pagination_key_asc(${1:string $column}, ${2:ParameterType|Type|string|int $type})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">pagination_key_asc</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$column</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ParameterType|Type|string|int</span> <span class=\"fn-param\">$type</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Key</span></code></pre>"

                },
                                {
                    name: "pagination_key_desc",
                    caption: "pagination_key_desc",
                    snippet: "\\Flow\\ETL\\Adapter\\Doctrine\\pagination_key_desc(${1:string $column}, ${2:ParameterType|Type|string|int $type})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">pagination_key_desc</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$column</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ParameterType|Type|string|int</span> <span class=\"fn-param\">$type</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Key</span></code></pre>"

                },
                                {
                    name: "pagination_key_set",
                    caption: "pagination_key_set",
                    snippet: "\\Flow\\ETL\\Adapter\\Doctrine\\pagination_key_set(${1:Key $keys})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">pagination_key_set</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Key</span> <span class=\"fn-param\">$keys</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">KeySet</span></code></pre>"

                },
                                {
                    name: "partition",
                    caption: "partition",
                    snippet: "\\Flow\\Filesystem\\DSL\\partition(${1:string $name}, ${2:string $value})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">partition</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Partition</span></code></pre>"

                },
                                {
                    name: "partitions",
                    caption: "partitions",
                    snippet: "\\Flow\\Filesystem\\DSL\\partitions(${1:Partition $partition})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">partitions</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Partition</span> <span class=\"fn-param\">$partition</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Partitions</span></code></pre>"

                },
                                {
                    name: "path",
                    caption: "path",
                    snippet: "\\Flow\\Filesystem\\DSL\\path(${1:string $path}, ${2:Options|array $options})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">path</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Options|array</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Path</span></code></pre><p>Path supports glob patterns.<br>Examples:<br> - path(\'*.csv\') - any csv file in current directory<br> - path(\'/** / *.csv\') - any csv file in any subdirectory (remove empty spaces)<br> - path(\'/dir/partition=* /*.parquet\') - any parquet file in given partition directory.<br>Glob pattern is also supported by remote filesystems like Azure<br> - path(\'azure-blob://directory/*.csv\') - any csv file in given directory<br>@param array<string, null|bool|float|int|string|\UnitEnum>|Path\Options $options</p>"

                },
                                {
                    name: "path_memory",
                    caption: "path_memory",
                    snippet: "\\Flow\\Filesystem\\DSL\\path_memory(${1:string $path}, ${2:array $options})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">path_memory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$path</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Path</span></code></pre><p>Create a path to php memory stream.<br>@param string $path - default = \'\' - path is used as an identifier in memory filesystem, so we can write multiple files to memory at once, each path is a new handle<br>@param null|array{\'stream\': \'memory\'|\'temp\'} $options - when nothing is provided, \'temp\' stream is used by default<br>@return Path</p>"

                },
                                {
                    name: "path_real",
                    caption: "path_real",
                    snippet: "\\Flow\\Filesystem\\DSL\\path_real(${1:string $path}, ${2:array $options})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">path_real</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Path</span></code></pre><p>Resolve real path from given path.<br>@param array<string, null|bool|float|int|string|\UnitEnum> $options</p>"

                },
                                {
                    name: "path_stdout",
                    caption: "path_stdout",
                    snippet: "\\Flow\\Filesystem\\DSL\\path_stdout(${1:array $options})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">path_stdout</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Path</span></code></pre><p>Create a path to php stdout stream.<br>@param null|array{\'stream\': \'output\'|\'stderr\'|\'stdout\'} $options<br>@return Path</p>"

                },
                                {
                    name: "pie_chart",
                    caption: "pie_chart",
                    snippet: "\\Flow\\ETL\\Adapter\\ChartJS\\pie_chart(${1:EntryReference $label}, ${2:References $datasets})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">pie_chart</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference</span> <span class=\"fn-param\">$label</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">References</span> <span class=\"fn-param\">$datasets</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PieChart</span></code></pre>"

                },
                                {
                    name: "postgresql_insert_options",
                    caption: "postgresql_insert_options",
                    snippet: "\\Flow\\ETL\\Adapter\\Doctrine\\postgresql_insert_options(${1:bool $skip_conflicts}, ${2:string $constraint}, ${3:array $conflict_columns}, ${4:array $update_columns})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">postgresql_insert_options</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">bool</span> <span class=\"fn-param\">$skip_conflicts</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$constraint</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$conflict_columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$update_columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSQLInsertOptions</span></code></pre><p>@param array<string> $conflict_columns<br>@param array<string> $update_columns</p>"

                },
                                {
                    name: "postgresql_update_options",
                    caption: "postgresql_update_options",
                    snippet: "\\Flow\\ETL\\Adapter\\Doctrine\\postgresql_update_options(${1:array $primary_key_columns}, ${2:array $update_columns})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">postgresql_update_options</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$primary_key_columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$update_columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSQLUpdateOptions</span></code></pre><p>@param array<string> $primary_key_columns<br>@param array<string> $update_columns</p>"

                },
                                {
                    name: "print_rows",
                    caption: "print_rows",
                    snippet: "\\Flow\\ETL\\DSL\\print_rows(${1:Rows $rows}, ${2:int|bool $truncate}, ${3:Formatter $formatter})",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">print_rows</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Rows</span> <span class=\"fn-param\">$rows</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int|bool</span> <span class=\"fn-param\">$truncate</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Formatter</span> <span class=\"fn-param\">$formatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span></code></pre>"

                },
                                {
                    name: "print_schema",
                    caption: "print_schema",
                    snippet: "\\Flow\\ETL\\DSL\\print_schema(${1:Schema $schema}, ${2:SchemaFormatter $formatter})",
                    meta: "flow-dsl-schema",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">print_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SchemaFormatter</span> <span class=\"fn-param\">$formatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span></code></pre><p>@param Schema $schema<br>@deprecated Please use schema_to_ascii($schema) instead</p>"

                },
                                {
                    name: "protocol",
                    caption: "protocol",
                    snippet: "\\Flow\\Filesystem\\DSL\\protocol(${1:string $protocol})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">protocol</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$protocol</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Protocol</span></code></pre>"

                },
                                {
                    name: "random_string",
                    caption: "random_string",
                    snippet: "\\Flow\\ETL\\DSL\\random_string(${1:ScalarFunction|int $length}, ${2:RandomValueGenerator $generator})",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">random_string</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$length</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">RandomValueGenerator</span> <span class=\"fn-param\">$generator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RandomString</span></code></pre>"

                },
                                {
                    name: "rank",
                    caption: "rank",
                    snippet: "\\Flow\\ETL\\DSL\\rank()",
                    meta: "flow-dsl-window-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">rank</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Rank</span></code></pre>"

                },
                                {
                    name: "ref",
                    caption: "ref",
                    snippet: "\\Flow\\ETL\\DSL\\ref(${1:string $entry})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">ref</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">EntryReference</span></code></pre>"

                },
                                {
                    name: "refs",
                    caption: "refs",
                    snippet: "\\Flow\\ETL\\DSL\\refs(${1:Reference|string $entries})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">refs</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$entries</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">References</span></code></pre>"

                },
                                {
                    name: "regex",
                    caption: "regex",
                    snippet: "\\Flow\\ETL\\DSL\\regex(${1:ScalarFunction|string $pattern}, ${2:ScalarFunction|string $subject}, ${3:ScalarFunction|int $flags}, ${4:ScalarFunction|int $offset})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">regex</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$subject</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$flags</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Regex</span></code></pre>"

                },
                                {
                    name: "regex_all",
                    caption: "regex_all",
                    snippet: "\\Flow\\ETL\\DSL\\regex_all(${1:ScalarFunction|string $pattern}, ${2:ScalarFunction|string $subject}, ${3:ScalarFunction|int $flags}, ${4:ScalarFunction|int $offset})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">regex_all</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$subject</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$flags</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RegexAll</span></code></pre>"

                },
                                {
                    name: "regex_match",
                    caption: "regex_match",
                    snippet: "\\Flow\\ETL\\DSL\\regex_match(${1:ScalarFunction|string $pattern}, ${2:ScalarFunction|string $subject}, ${3:ScalarFunction|int $flags}, ${4:ScalarFunction|int $offset})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">regex_match</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$subject</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$flags</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RegexMatch</span></code></pre>"

                },
                                {
                    name: "regex_match_all",
                    caption: "regex_match_all",
                    snippet: "\\Flow\\ETL\\DSL\\regex_match_all(${1:ScalarFunction|string $pattern}, ${2:ScalarFunction|string $subject}, ${3:ScalarFunction|int $flags}, ${4:ScalarFunction|int $offset})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">regex_match_all</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$subject</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$flags</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RegexMatchAll</span></code></pre>"

                },
                                {
                    name: "regex_replace",
                    caption: "regex_replace",
                    snippet: "\\Flow\\ETL\\DSL\\regex_replace(${1:ScalarFunction|string $pattern}, ${2:ScalarFunction|string $replacement}, ${3:ScalarFunction|string $subject}, ${4:ScalarFunction|int|null $limit})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">regex_replace</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$replacement</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$subject</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int|null</span> <span class=\"fn-param\">$limit</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RegexReplace</span></code></pre>"

                },
                                {
                    name: "rename_replace",
                    caption: "rename_replace",
                    snippet: "\\Flow\\ETL\\DSL\\rename_replace(${1:array|string $search}, ${2:array|string $replace})",
                    meta: "flow-dsl-transformers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">rename_replace</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array|string</span> <span class=\"fn-param\">$search</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array|string</span> <span class=\"fn-param\">$replace</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RenameReplaceEntryStrategy</span></code></pre><p>@param array<string>|string $search<br>@param array<string>|string $replace</p>"

                },
                                {
                    name: "rename_style",
                    caption: "rename_style",
                    snippet: "\\Flow\\ETL\\DSL\\rename_style(${1:StringStyles|StringStyles $style})",
                    meta: "flow-dsl-transformers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">rename_style</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">StringStyles|StringStyles</span> <span class=\"fn-param\">$style</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RenameCaseEntryStrategy</span></code></pre>"

                },
                                {
                    name: "retry_any_throwable",
                    caption: "retry_any_throwable",
                    snippet: "\\Flow\\ETL\\DSL\\retry_any_throwable(${1:int $limit})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">retry_any_throwable</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$limit</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AnyThrowable</span></code></pre>"

                },
                                {
                    name: "retry_on_exception_types",
                    caption: "retry_on_exception_types",
                    snippet: "\\Flow\\ETL\\DSL\\retry_on_exception_types(${1:array $exception_types}, ${2:int $limit})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">retry_on_exception_types</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$exception_types</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$limit</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OnExceptionTypes</span></code></pre><p>@param array<class-string<\Throwable>> $exception_types</p>"

                },
                                {
                    name: "round",
                    caption: "round",
                    snippet: "\\Flow\\ETL\\DSL\\round(${1:ScalarFunction|int|float $value}, ${2:ScalarFunction|int $precision}, ${3:ScalarFunction|int $mode})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">round</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|int|float</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$precision</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$mode</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Round</span></code></pre>"

                },
                                {
                    name: "row",
                    caption: "row",
                    snippet: "\\Flow\\ETL\\DSL\\row(${1:Entry $entry})",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">row</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Entry</span> <span class=\"fn-param\">$entry</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Row</span></code></pre><p>@param Entry<mixed> ...$entry</p>"

                },
                                {
                    name: "rows",
                    caption: "rows",
                    snippet: "\\Flow\\ETL\\DSL\\rows(${1:Row $row})",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">rows</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Row</span> <span class=\"fn-param\">$row</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Rows</span></code></pre>"

                },
                                {
                    name: "rows_partitioned",
                    caption: "rows_partitioned",
                    snippet: "\\Flow\\ETL\\DSL\\rows_partitioned(${1:array $rows}, ${2:Partitions|array $partitions})",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">rows_partitioned</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$rows</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Partitions|array</span> <span class=\"fn-param\">$partitions</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Rows</span></code></pre><p>@param array<Row> $rows<br>@param array<\Flow\Filesystem\Partition|string>|Partitions $partitions</p>"

                },
                                {
                    name: "row_number",
                    caption: "row_number",
                    snippet: "\\Flow\\ETL\\DSL\\row_number()",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">row_number</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RowNumber</span></code></pre>"

                },
                                {
                    name: "sanitize",
                    caption: "sanitize",
                    snippet: "\\Flow\\ETL\\DSL\\sanitize(${1:ScalarFunction|string $value}, ${2:ScalarFunction|string $placeholder}, ${3:ScalarFunction|int|null $skipCharacters})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">sanitize</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$placeholder</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int|null</span> <span class=\"fn-param\">$skipCharacters</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Sanitize</span></code></pre>"

                },
                                {
                    name: "schema",
                    caption: "schema",
                    snippet: "\\Flow\\ETL\\DSL\\schema(${1:Definition $definitions})",
                    meta: "flow-dsl-schema",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Definition</span> <span class=\"fn-param\">$definitions</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Schema</span></code></pre><p>@param Definition<mixed> ...$definitions<br>@return Schema</p>"

                },
                                {
                    name: "schema_evolving_validator",
                    caption: "schema_evolving_validator",
                    snippet: "\\Flow\\ETL\\DSL\\schema_evolving_validator()",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">schema_evolving_validator</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">EvolvingValidator</span></code></pre>"

                },
                                {
                    name: "schema_from_json",
                    caption: "schema_from_json",
                    snippet: "\\Flow\\ETL\\DSL\\schema_from_json(${1:string $schema})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">schema_from_json</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Schema</span></code></pre><p>@return Schema</p>"

                },
                                {
                    name: "schema_from_parquet",
                    caption: "schema_from_parquet",
                    snippet: "\\Flow\\ETL\\Adapter\\Parquet\\schema_from_parquet(${1:Schema $schema})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">schema_from_parquet</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Schema</span></code></pre>"

                },
                                {
                    name: "schema_metadata",
                    caption: "schema_metadata",
                    snippet: "\\Flow\\ETL\\DSL\\schema_metadata(${1:array $metadata})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">schema_metadata</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Metadata</span></code></pre><p>@param array<string, array<bool|float|int|string>|bool|float|int|string> $metadata</p>"

                },
                                {
                    name: "schema_selective_validator",
                    caption: "schema_selective_validator",
                    snippet: "\\Flow\\ETL\\DSL\\schema_selective_validator()",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">schema_selective_validator</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SelectiveValidator</span></code></pre>"

                },
                                {
                    name: "schema_strict_validator",
                    caption: "schema_strict_validator",
                    snippet: "\\Flow\\ETL\\DSL\\schema_strict_validator()",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">schema_strict_validator</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StrictValidator</span></code></pre>"

                },
                                {
                    name: "schema_to_ascii",
                    caption: "schema_to_ascii",
                    snippet: "\\Flow\\ETL\\DSL\\schema_to_ascii(${1:Schema $schema}, ${2:SchemaFormatter $formatter})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">schema_to_ascii</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SchemaFormatter</span> <span class=\"fn-param\">$formatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span></code></pre><p>@param Schema $schema</p>"

                },
                                {
                    name: "schema_to_json",
                    caption: "schema_to_json",
                    snippet: "\\Flow\\ETL\\DSL\\schema_to_json(${1:Schema $schema}, ${2:bool $pretty})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">schema_to_json</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$pretty</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span></code></pre><p>@param Schema $schema</p>"

                },
                                {
                    name: "schema_to_parquet",
                    caption: "schema_to_parquet",
                    snippet: "\\Flow\\ETL\\Adapter\\Parquet\\schema_to_parquet(${1:Schema $schema})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">schema_to_parquet</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Schema</span></code></pre>"

                },
                                {
                    name: "schema_to_php",
                    caption: "schema_to_php",
                    snippet: "\\Flow\\ETL\\DSL\\schema_to_php(${1:Schema $schema}, ${2:ValueFormatter $valueFormatter}, ${3:TypeFormatter $typeFormatter})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">schema_to_php</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ValueFormatter</span> <span class=\"fn-param\">$valueFormatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">TypeFormatter</span> <span class=\"fn-param\">$typeFormatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span></code></pre><p>@param Schema $schema</p>"

                },
                                {
                    name: "schema_validate",
                    caption: "schema_validate",
                    snippet: "\\Flow\\ETL\\DSL\\schema_validate(${1:Schema $expected}, ${2:Schema $given}, ${3:SchemaValidator $validator})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">schema_validate</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$expected</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$given</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SchemaValidator</span> <span class=\"fn-param\">$validator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">bool</span></code></pre><p>@param Schema $expected<br>@param Schema $given</p>"

                },
                                {
                    name: "select",
                    caption: "select",
                    snippet: "\\Flow\\ETL\\DSL\\select(${1:Reference|string $entries})",
                    meta: "flow-dsl-transformers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">select</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$entries</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Select</span></code></pre>"

                },
                                {
                    name: "size",
                    caption: "size",
                    snippet: "\\Flow\\ETL\\DSL\\size(${1:mixed $value})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">size</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Size</span></code></pre>"

                },
                                {
                    name: "skip_rows_handler",
                    caption: "skip_rows_handler",
                    snippet: "\\Flow\\ETL\\DSL\\skip_rows_handler()",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">skip_rows_handler</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SkipRows</span></code></pre>"

                },
                                {
                    name: "split",
                    caption: "split",
                    snippet: "\\Flow\\ETL\\DSL\\split(${1:ScalarFunction|string $value}, ${2:ScalarFunction|string $separator}, ${3:ScalarFunction|int $limit})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">split</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$separator</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$limit</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Split</span></code></pre>"

                },
                                {
                    name: "sprintf",
                    caption: "sprintf",
                    snippet: "\\Flow\\ETL\\DSL\\sprintf(${1:ScalarFunction|string $format}, ${2:ScalarFunction|string|int|float|null $args})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">sprintf</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$format</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string|int|float|null</span> <span class=\"fn-param\">$args</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Sprintf</span></code></pre>"

                },
                                {
                    name: "sqlite_insert_options",
                    caption: "sqlite_insert_options",
                    snippet: "\\Flow\\ETL\\Adapter\\Doctrine\\sqlite_insert_options(${1:bool $skip_conflicts}, ${2:array $conflict_columns}, ${3:array $update_columns})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">sqlite_insert_options</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">bool</span> <span class=\"fn-param\">$skip_conflicts</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$conflict_columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$update_columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SqliteInsertOptions</span></code></pre><p>@param array<string> $conflict_columns<br>@param array<string> $update_columns</p>"

                },
                                {
                    name: "stdout_filesystem",
                    caption: "stdout_filesystem",
                    snippet: "\\Flow\\Filesystem\\DSL\\stdout_filesystem()",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">stdout_filesystem</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StdOutFilesystem</span></code></pre><p>Write-only filesystem useful when we just want to write the output to stdout.<br>The main use case is for streaming datasets over http.</p>"

                },
                                {
                    name: "string_agg",
                    caption: "string_agg",
                    snippet: "\\Flow\\ETL\\DSL\\string_agg(${1:EntryReference|string $ref}, ${2:string $separator}, ${3:SortOrder $sort})",
                    meta: "flow-dsl-aggregating-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">string_agg</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$separator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SortOrder</span> <span class=\"fn-param\">$sort</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StringAggregate</span></code></pre>"

                },
                                {
                    name: "string_entry",
                    caption: "string_entry",
                    snippet: "\\Flow\\ETL\\DSL\\string_entry(${1:string $name}, ${2:string $value}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-entries",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">string_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span></code></pre><p>@return Entry<?string></p>"

                },
                                {
                    name: "string_schema",
                    caption: "string_schema",
                    snippet: "\\Flow\\ETL\\DSL\\string_schema(${1:string $name}, ${2:bool $nullable}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-schema",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">string_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span></code></pre><p>@return Definition<string></p>"

                },
                                {
                    name: "structure_entry",
                    caption: "structure_entry",
                    snippet: "\\Flow\\ETL\\DSL\\structure_entry(${1:string $name}, ${2:array $value}, ${3:Type $type}, ${4:Metadata $metadata})",
                    meta: "flow-dsl-entries",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">structure_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span></code></pre><p>@template T<br>@param ?array<string, mixed> $value<br>@param Type<array<string, T>> $type<br>@return Entry<?array<string, T>></p>"

                },
                                {
                    name: "structure_ref",
                    caption: "structure_ref",
                    snippet: "\\Flow\\ETL\\DSL\\structure_ref(${1:string $entry})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">structure_ref</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StructureFunctions</span></code></pre>"

                },
                                {
                    name: "structure_schema",
                    caption: "structure_schema",
                    snippet: "\\Flow\\ETL\\DSL\\structure_schema(${1:string $name}, ${2:Type $type}, ${3:bool $nullable}, ${4:Metadata $metadata})",
                    meta: "flow-dsl-schema",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">structure_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span></code></pre><p>@template T<br>@param Type<T> $type<br>@return Definition<T></p>"

                },
                                {
                    name: "struct_entry",
                    caption: "struct_entry",
                    snippet: "\\Flow\\ETL\\DSL\\struct_entry(${1:string $name}, ${2:array $value}, ${3:Type $type}, ${4:Metadata $metadata})",
                    meta: "flow-dsl-entries",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">struct_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span></code></pre><p>@template T<br>@param ?array<string, mixed> $value<br>@param Type<array<string, T>> $type<br>@return Entry<?array<string, T>></p>"

                },
                                {
                    name: "struct_schema",
                    caption: "struct_schema",
                    snippet: "\\Flow\\ETL\\DSL\\struct_schema(${1:string $name}, ${2:Type $type}, ${3:bool $nullable}, ${4:Metadata $metadata})",
                    meta: "flow-dsl-schema",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">struct_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span></code></pre><p>@template T<br>@param Type<T> $type<br>@return Definition<T><br>@deprecated Use `structure_schema()` instead</p>"

                },
                                {
                    name: "str_entry",
                    caption: "str_entry",
                    snippet: "\\Flow\\ETL\\DSL\\str_entry(${1:string $name}, ${2:string $value}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-entries",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">str_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span></code></pre><p>@return Entry<?string></p>"

                },
                                {
                    name: "str_schema",
                    caption: "str_schema",
                    snippet: "\\Flow\\ETL\\DSL\\str_schema(${1:string $name}, ${2:bool $nullable}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-schema",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">str_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span></code></pre><p>Alias for `string_schema`.<br>@return Definition<string></p>"

                },
                                {
                    name: "sum",
                    caption: "sum",
                    snippet: "\\Flow\\ETL\\DSL\\sum(${1:EntryReference|string $ref})",
                    meta: "flow-dsl-aggregating-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">sum</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Sum</span></code></pre>"

                },
                                {
                    name: "table_schema_to_flow_schema",
                    caption: "table_schema_to_flow_schema",
                    snippet: "\\Flow\\ETL\\Adapter\\Doctrine\\table_schema_to_flow_schema(${1:Table $table}, ${2:array $types_map})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">table_schema_to_flow_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Table</span> <span class=\"fn-param\">$table</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$types_map</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Schema</span></code></pre><p>Converts a Doctrine\DBAL\Schema\Table to a Flow\ETL\Schema.<br>@param array<class-string<\Flow\Types\Type<mixed>>, class-string<\Doctrine\DBAL\Types\Type>> $types_map<br>@return Schema</p>"

                },
                                {
                    name: "throw_error_handler",
                    caption: "throw_error_handler",
                    snippet: "\\Flow\\ETL\\DSL\\throw_error_handler()",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">throw_error_handler</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ThrowError</span></code></pre>"

                },
                                {
                    name: "time_entry",
                    caption: "time_entry",
                    snippet: "\\Flow\\ETL\\DSL\\time_entry(${1:string $name}, ${2:DateInterval|string|null $value}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-entries",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">time_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DateInterval|string|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span></code></pre><p>@return Entry<?\DateInterval></p>"

                },
                                {
                    name: "time_schema",
                    caption: "time_schema",
                    snippet: "\\Flow\\ETL\\DSL\\time_schema(${1:string $name}, ${2:bool $nullable}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-schema",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">time_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span></code></pre><p>@return Definition<\DateInterval></p>"

                },
                                {
                    name: "to_array",
                    caption: "to_array",
                    snippet: "\\Flow\\ETL\\DSL\\to_array(${1:array $array})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_array</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$array</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayLoader</span></code></pre><p>Convert rows to an array and store them in passed array variable.<br>@param array<array-key, mixed> $array<br>@param-out array<array<mixed>> $array</p>"

                },
                                {
                    name: "to_avro",
                    caption: "to_avro",
                    snippet: "\\Flow\\ETL\\DSL\\Adapter\\Avro\\to_avro(${1:Path|string $path}, ${2:Schema $schema})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_avro</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AvroLoader</span></code></pre>"

                },
                                {
                    name: "to_branch",
                    caption: "to_branch",
                    snippet: "\\Flow\\ETL\\DSL\\to_branch(${1:ScalarFunction $condition}, ${2:Loader $loader})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_branch</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$condition</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Loader</span> <span class=\"fn-param\">$loader</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BranchingLoader</span></code></pre>"

                },
                                {
                    name: "to_callable",
                    caption: "to_callable",
                    snippet: "\\Flow\\ETL\\DSL\\to_callable(${1:callable $callable})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_callable</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">callable</span> <span class=\"fn-param\">$callable</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CallbackLoader</span></code></pre>"

                },
                                {
                    name: "to_chartjs",
                    caption: "to_chartjs",
                    snippet: "\\Flow\\ETL\\Adapter\\ChartJS\\to_chartjs(${1:Chart $type})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_chartjs</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Chart</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ChartJSLoader</span></code></pre>"

                },
                                {
                    name: "to_chartjs_file",
                    caption: "to_chartjs_file",
                    snippet: "\\Flow\\ETL\\Adapter\\ChartJS\\to_chartjs_file(${1:Chart $type}, ${2:Path|string|null $output}, ${3:Path|string|null $template})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_chartjs_file</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Chart</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Path|string|null</span> <span class=\"fn-param\">$output</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Path|string|null</span> <span class=\"fn-param\">$template</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ChartJSLoader</span></code></pre><p>@param Chart $type<br>@param null|Path|string $output - @deprecated use $loader->withOutputPath() instead<br>@param null|Path|string $template - @deprecated use $loader->withTemplate() instead</p>"

                },
                                {
                    name: "to_chartjs_var",
                    caption: "to_chartjs_var",
                    snippet: "\\Flow\\ETL\\Adapter\\ChartJS\\to_chartjs_var(${1:Chart $type}, ${2:array $output})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_chartjs_var</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Chart</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$output</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ChartJSLoader</span></code></pre><p>@param Chart $type<br>@param array<array-key, mixed> $output - @deprecated use $loader->withOutputVar() instead</p>"

                },
                                {
                    name: "to_csv",
                    caption: "to_csv",
                    snippet: "\\Flow\\ETL\\Adapter\\CSV\\to_csv(${1:Path|string $uri}, ${2:bool $with_header}, ${3:string $separator}, ${4:string $enclosure}, ${5:string $escape}, ${6:string $new_line_separator}, ${7:string $datetime_format})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_csv</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$uri</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$with_header</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$separator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$enclosure</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$escape</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$new_line_separator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$datetime_format</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CSVLoader</span></code></pre><p>@param Path|string $uri<br>@param bool $with_header - @deprecated use $loader->withHeader() instead<br>@param string $separator - @deprecated use $loader->withSeparator() instead<br>@param string $enclosure - @deprecated use $loader->withEnclosure() instead<br>@param string $escape - @deprecated use $loader->withEscape() instead<br>@param string $new_line_separator - @deprecated use $loader->withNewLineSeparator() instead<br>@param string $datetime_format - @deprecated use $loader->withDateTimeFormat() instead</p>"

                },
                                {
                    name: "to_date",
                    caption: "to_date",
                    snippet: "\\Flow\\ETL\\DSL\\to_date(${1:mixed $ref}, ${2:ScalarFunction|string $format}, ${3:ScalarFunction|DateTimeZone $timeZone})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_date</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$format</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|DateTimeZone</span> <span class=\"fn-param\">$timeZone</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ToDate</span></code></pre>"

                },
                                {
                    name: "to_date_time",
                    caption: "to_date_time",
                    snippet: "\\Flow\\ETL\\DSL\\to_date_time(${1:mixed $ref}, ${2:ScalarFunction|string $format}, ${3:ScalarFunction|DateTimeZone $timeZone})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_date_time</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$format</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|DateTimeZone</span> <span class=\"fn-param\">$timeZone</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ToDateTime</span></code></pre>"

                },
                                {
                    name: "to_dbal_schema_table",
                    caption: "to_dbal_schema_table",
                    snippet: "\\Flow\\ETL\\Adapter\\Doctrine\\to_dbal_schema_table(${1:Schema $schema}, ${2:string $table_name}, ${3:array $table_options}, ${4:array $types_map})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_dbal_schema_table</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$table_name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$table_options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$types_map</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Table</span></code></pre><p>Converts a Flow\ETL\Schema to a Doctrine\DBAL\Schema\Table.<br>@param Schema $schema<br>@param array<array-key, mixed> $table_options<br>@param array<class-string<\Flow\Types\Type<mixed>>, class-string<\Doctrine\DBAL\Types\Type>> $types_map</p>"

                },
                                {
                    name: "to_dbal_table_delete",
                    caption: "to_dbal_table_delete",
                    snippet: "\\Flow\\ETL\\Adapter\\Doctrine\\to_dbal_table_delete(${1:Connection|array $connection}, ${2:string $table})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_dbal_table_delete</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection|array</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$table</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalLoader</span></code></pre><p>Delete rows from database table based on the provided data.<br>In order to control the size of the single request, use DataFrame::chunkSize() method just before calling DataFrame::load().<br>@param array<string, mixed>|Connection $connection<br>@throws InvalidArgumentException</p>"

                },
                                {
                    name: "to_dbal_table_insert",
                    caption: "to_dbal_table_insert",
                    snippet: "\\Flow\\ETL\\Adapter\\Doctrine\\to_dbal_table_insert(${1:Connection|array $connection}, ${2:string $table}, ${3:InsertOptions $options})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_dbal_table_insert</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection|array</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$table</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">InsertOptions</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalLoader</span></code></pre><p>Insert new rows into a database table.<br>Insert can also be used as an upsert with the help of InsertOptions.<br>InsertOptions are platform specific, so please choose the right one for your database.<br> - MySQLInsertOptions<br> - PostgreSQLInsertOptions<br> - SqliteInsertOptions<br>In order to control the size of the single insert, use DataFrame::chunkSize() method just before calling DataFrame::load().<br>@param array<string, mixed>|Connection $connection<br>@throws InvalidArgumentException</p>"

                },
                                {
                    name: "to_dbal_table_update",
                    caption: "to_dbal_table_update",
                    snippet: "\\Flow\\ETL\\Adapter\\Doctrine\\to_dbal_table_update(${1:Connection|array $connection}, ${2:string $table}, ${3:UpdateOptions $options})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_dbal_table_update</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection|array</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$table</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">UpdateOptions</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalLoader</span></code></pre><p> Update existing rows in database.<br> In order to control the size of the single request, use DataFrame::chunkSize() method just before calling DataFrame::load().<br>@param array<string, mixed>|Connection $connection<br>@throws InvalidArgumentException</p>"

                },
                                {
                    name: "to_dbal_transaction",
                    caption: "to_dbal_transaction",
                    snippet: "\\Flow\\ETL\\Adapter\\Doctrine\\to_dbal_transaction(${1:Connection|array $connection}, ${2:Loader $loaders})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_dbal_transaction</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection|array</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Loader</span> <span class=\"fn-param\">$loaders</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">TransactionalDbalLoader</span></code></pre><p>Execute multiple loaders within a database transaction.<br>Each batch of rows will be processed in its own transaction.<br>If any loader fails, the entire batch will be rolled back.<br>@param array<string, mixed>|Connection $connection<br>@param Loader ...$loaders - Loaders to execute within the transaction<br>@throws InvalidArgumentException</p>"

                },
                                {
                    name: "to_entry",
                    caption: "to_entry",
                    snippet: "\\Flow\\ETL\\DSL\\to_entry(${1:string $name}, ${2:mixed $data}, ${3:EntryFactory $entryFactory})",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$data</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">EntryFactory</span> <span class=\"fn-param\">$entryFactory</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span></code></pre><p>@param array<mixed> $data<br>@return Entry<mixed></p>"

                },
                                {
                    name: "to_es_bulk_index",
                    caption: "to_es_bulk_index",
                    snippet: "\\Flow\\ETL\\Adapter\\Elasticsearch\\to_es_bulk_index(${1:array $config}, ${2:string $index}, ${3:IdFactory $id_factory}, ${4:array $parameters})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_es_bulk_index</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$config</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$index</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">IdFactory</span> <span class=\"fn-param\">$id_factory</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$parameters</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ElasticsearchLoader</span></code></pre><p>https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html.<br>In order to control the size of the single request, use DataFrame::chunkSize() method just before calling DataFrame::load().<br>@param array{<br> hosts?: array<string>,<br> connectionParams?: array<mixed>,<br> retries?: int,<br> sniffOnStart?: bool,<br> sslCert?: array<string>,<br> sslKey?: array<string>,<br> sslVerification?: bool|string,<br> elasticMetaHeader?: bool,<br> includePortInHostHeader?: bool<br>} $config<br>@param string $index<br>@param IdFactory $id_factory<br>@param array<mixed> $parameters - https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html - @deprecated use withParameters method instead</p>"

                },
                                {
                    name: "to_es_bulk_update",
                    caption: "to_es_bulk_update",
                    snippet: "\\Flow\\ETL\\Adapter\\Elasticsearch\\to_es_bulk_update(${1:array $config}, ${2:string $index}, ${3:IdFactory $id_factory}, ${4:array $parameters})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_es_bulk_update</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$config</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$index</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">IdFactory</span> <span class=\"fn-param\">$id_factory</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$parameters</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ElasticsearchLoader</span></code></pre><p> https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html.<br>In order to control the size of the single request, use DataFrame::chunkSize() method just before calling DataFrame::load().<br>@param array{<br> hosts?: array<string>,<br> connectionParams?: array<mixed>,<br> retries?: int,<br> sniffOnStart?: bool,<br> sslCert?: array<string>,<br> sslKey?: array<string>,<br> sslVerification?: bool|string,<br> elasticMetaHeader?: bool,<br> includePortInHostHeader?: bool<br>} $config<br>@param string $index<br>@param IdFactory $id_factory<br>@param array<mixed> $parameters - https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html - @deprecated use withParameters method instead</p>"

                },
                                {
                    name: "to_json",
                    caption: "to_json",
                    snippet: "\\Flow\\ETL\\Adapter\\JSON\\to_json(${1:Path|string $path}, ${2:int $flags}, ${3:string $date_time_format}, ${4:bool $put_rows_in_new_lines})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_json</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$flags</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$date_time_format</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$put_rows_in_new_lines</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">JsonLoader</span></code></pre><p>@param Path|string $path<br>@param int $flags - PHP JSON Flags - @deprecate use withFlags method instead<br>@param string $date_time_format - format for DateTimeInterface::format() - @deprecate use withDateTimeFormat method instead<br>@param bool $put_rows_in_new_lines - if you want to put each row in a new line - @deprecate use withRowsInNewLines method instead<br>@return JsonLoader</p>"

                },
                                {
                    name: "to_json_lines",
                    caption: "to_json_lines",
                    snippet: "\\Flow\\ETL\\Adapter\\JSON\\to_json_lines(${1:Path|string $path})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_json_lines</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">JsonLinesLoader</span></code></pre><p>Used to write to a JSON lines https://jsonlines.org/ formatted file.<br>@param Path|string $path<br>@return JsonLinesLoader</p>"

                },
                                {
                    name: "to_meilisearch_bulk_index",
                    caption: "to_meilisearch_bulk_index",
                    snippet: "\\Flow\\ETL\\Adapter\\Meilisearch\\to_meilisearch_bulk_index(${1:array $config}, ${2:string $index})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_meilisearch_bulk_index</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$config</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$index</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Loader</span></code></pre><p>@param array{url: string, apiKey: string, httpClient: ?ClientInterface} $config</p>"

                },
                                {
                    name: "to_meilisearch_bulk_update",
                    caption: "to_meilisearch_bulk_update",
                    snippet: "\\Flow\\ETL\\Adapter\\Meilisearch\\to_meilisearch_bulk_update(${1:array $config}, ${2:string $index})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_meilisearch_bulk_update</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$config</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$index</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Loader</span></code></pre><p>@param array{url: string, apiKey: string, httpClient: ?ClientInterface} $config</p>"

                },
                                {
                    name: "to_memory",
                    caption: "to_memory",
                    snippet: "\\Flow\\ETL\\DSL\\to_memory(${1:Memory $memory})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_memory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Memory</span> <span class=\"fn-param\">$memory</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MemoryLoader</span></code></pre>"

                },
                                {
                    name: "to_output",
                    caption: "to_output",
                    snippet: "\\Flow\\ETL\\DSL\\to_output(${1:int|bool $truncate}, ${2:Output $output}, ${3:Formatter $formatter}, ${4:SchemaFormatter $schemaFormatter})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_output</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int|bool</span> <span class=\"fn-param\">$truncate</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Output</span> <span class=\"fn-param\">$output</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Formatter</span> <span class=\"fn-param\">$formatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SchemaFormatter</span> <span class=\"fn-param\">$schemaFormatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StreamLoader</span></code></pre>"

                },
                                {
                    name: "to_parquet",
                    caption: "to_parquet",
                    snippet: "\\Flow\\ETL\\Adapter\\Parquet\\to_parquet(${1:Path|string $path}, ${2:Options $options}, ${3:Compressions $compressions}, ${4:Schema $schema})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_parquet</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Options</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Compressions</span> <span class=\"fn-param\">$compressions</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ParquetLoader</span></code></pre><p>@param Path|string $path<br>@param null|Options $options - @deprecated use `withOptions` method instead<br>@param Compressions $compressions - @deprecated use `withCompressions` method instead<br>@param null|Schema $schema - @deprecated use `withSchema` method instead</p>"

                },
                                {
                    name: "to_stderr",
                    caption: "to_stderr",
                    snippet: "\\Flow\\ETL\\DSL\\to_stderr(${1:int|bool $truncate}, ${2:Output $output}, ${3:Formatter $formatter}, ${4:SchemaFormatter $schemaFormatter})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_stderr</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int|bool</span> <span class=\"fn-param\">$truncate</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Output</span> <span class=\"fn-param\">$output</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Formatter</span> <span class=\"fn-param\">$formatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SchemaFormatter</span> <span class=\"fn-param\">$schemaFormatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StreamLoader</span></code></pre>"

                },
                                {
                    name: "to_stdout",
                    caption: "to_stdout",
                    snippet: "\\Flow\\ETL\\DSL\\to_stdout(${1:int|bool $truncate}, ${2:Output $output}, ${3:Formatter $formatter}, ${4:SchemaFormatter $schemaFormatter})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_stdout</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int|bool</span> <span class=\"fn-param\">$truncate</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Output</span> <span class=\"fn-param\">$output</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Formatter</span> <span class=\"fn-param\">$formatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SchemaFormatter</span> <span class=\"fn-param\">$schemaFormatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StreamLoader</span></code></pre>"

                },
                                {
                    name: "to_stream",
                    caption: "to_stream",
                    snippet: "\\Flow\\ETL\\DSL\\to_stream(${1:string $uri}, ${2:int|bool $truncate}, ${3:Output $output}, ${4:string $mode}, ${5:Formatter $formatter}, ${6:SchemaFormatter $schemaFormatter})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_stream</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$uri</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int|bool</span> <span class=\"fn-param\">$truncate</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Output</span> <span class=\"fn-param\">$output</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$mode</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Formatter</span> <span class=\"fn-param\">$formatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SchemaFormatter</span> <span class=\"fn-param\">$schemaFormatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StreamLoader</span></code></pre>"

                },
                                {
                    name: "to_text",
                    caption: "to_text",
                    snippet: "\\Flow\\ETL\\Adapter\\Text\\to_text(${1:Path|string $path}, ${2:string $new_line_separator})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_text</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$new_line_separator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Loader</span></code></pre><p>@param Path|string $path<br>@param string $new_line_separator - default PHP_EOL - @deprecated use withNewLineSeparator method instead<br>@return Loader</p>"

                },
                                {
                    name: "to_timezone",
                    caption: "to_timezone",
                    snippet: "\\Flow\\ETL\\DSL\\to_timezone(${1:ScalarFunction|DateTimeInterface $value}, ${2:ScalarFunction|DateTimeZone|string $timeZone})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_timezone</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|DateTimeInterface</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|DateTimeZone|string</span> <span class=\"fn-param\">$timeZone</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ToTimeZone</span></code></pre>"

                },
                                {
                    name: "to_transformation",
                    caption: "to_transformation",
                    snippet: "\\Flow\\ETL\\DSL\\to_transformation(${1:Transformer|Transformation $transformer}, ${2:Loader $loader})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_transformation</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Transformer|Transformation</span> <span class=\"fn-param\">$transformer</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Loader</span> <span class=\"fn-param\">$loader</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">TransformerLoader</span></code></pre>"

                },
                                {
                    name: "to_xml",
                    caption: "to_xml",
                    snippet: "\\Flow\\ETL\\Adapter\\XML\\to_xml(${1:Path|string $path}, ${2:string $root_element_name}, ${3:string $row_element_name}, ${4:string $attribute_prefix}, ${5:string $date_time_format}, ${6:XMLWriter $xml_writer})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">to_xml</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$root_element_name</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$row_element_name</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$attribute_prefix</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$date_time_format</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">XMLWriter</span> <span class=\"fn-param\">$xml_writer</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">XMLLoader</span></code></pre><p>@param Path|string $path<br>@param string $root_element_name - @deprecated use `withRootElementName()` method instead<br>@param string $row_element_name - @deprecated use `withRowElementName()` method instead<br>@param string $attribute_prefix - @deprecated use `withAttributePrefix()` method instead<br>@param string $date_time_format - @deprecated use `withDateTimeFormat()` method instead<br>@param XMLWriter $xml_writer</p>"

                },
                                {
                    name: "types",
                    caption: "types",
                    snippet: "\\Flow\\Types\\DSL\\types(${1:Type $types})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">types</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$types</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Types</span></code></pre><p>@template T<br>@param Type<T> ...$types<br>@return Types<T></p>"

                },
                                {
                    name: "type_array",
                    caption: "type_array",
                    snippet: "\\Flow\\Types\\DSL\\type_array()",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_array</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@return Type<array<mixed>></p>"

                },
                                {
                    name: "type_boolean",
                    caption: "type_boolean",
                    snippet: "\\Flow\\Types\\DSL\\type_boolean()",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_boolean</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@return Type<bool></p>"

                },
                                {
                    name: "type_callable",
                    caption: "type_callable",
                    snippet: "\\Flow\\Types\\DSL\\type_callable()",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_callable</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@return Type<callable></p>"

                },
                                {
                    name: "type_class_string",
                    caption: "type_class_string",
                    snippet: "\\Flow\\Types\\DSL\\type_class_string(${1:string $class})",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_class_string</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$class</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@template T of object<br>@param null|class-string<T> $class<br>@return ($class is null ? Type<class-string> : Type<class-string<T>>)</p>"

                },
                                {
                    name: "type_date",
                    caption: "type_date",
                    snippet: "\\Flow\\ETL\\DSL\\type_date()",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_date</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@deprecated please use \Flow\Types\DSL\type_date() : DateType<br>@return Type<\DateTimeInterface></p>"

                },
                                {
                    name: "type_date",
                    caption: "type_date",
                    snippet: "\\Flow\\Types\\DSL\\type_date()",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_date</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@return Type<\DateTimeInterface></p>"

                },
                                {
                    name: "type_datetime",
                    caption: "type_datetime",
                    snippet: "\\Flow\\Types\\DSL\\type_datetime()",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_datetime</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@return Type<\DateTimeInterface></p>"

                },
                                {
                    name: "type_enum",
                    caption: "type_enum",
                    snippet: "\\Flow\\Types\\DSL\\type_enum(${1:string $class})",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_enum</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$class</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@template T of UnitEnum<br>@param class-string<T> $class<br>@return Type<T></p>"

                },
                                {
                    name: "type_equals",
                    caption: "type_equals",
                    snippet: "\\Flow\\Types\\DSL\\type_equals(${1:Type $left}, ${2:Type $right})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_equals</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$right</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">bool</span></code></pre><p>@param Type<mixed> $left<br>@param Type<mixed> $right</p>"

                },
                                {
                    name: "type_float",
                    caption: "type_float",
                    snippet: "\\Flow\\Types\\DSL\\type_float()",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_float</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@return Type<float></p>"

                },
                                {
                    name: "type_from_array",
                    caption: "type_from_array",
                    snippet: "\\Flow\\Types\\DSL\\type_from_array(${1:array $data})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_from_array</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$data</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@param array<string, mixed> $data<br>@return Type<mixed></p>"

                },
                                {
                    name: "type_html",
                    caption: "type_html",
                    snippet: "\\Flow\\Types\\DSL\\type_html()",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_html</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@return Type<HTMLDocument></p>"

                },
                                {
                    name: "type_instance_of",
                    caption: "type_instance_of",
                    snippet: "\\Flow\\Types\\DSL\\type_instance_of(${1:string $class})",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_instance_of</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$class</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@template T of object<br>@param class-string<T> $class<br>@return Type<T></p>"

                },
                                {
                    name: "type_int",
                    caption: "type_int",
                    snippet: "\\Flow\\ETL\\DSL\\type_int()",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_int</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@deprecated please use \Flow\Types\DSL\type_integer() : IntegerType<br>@return Type<int></p>"

                },
                                {
                    name: "type_integer",
                    caption: "type_integer",
                    snippet: "\\Flow\\Types\\DSL\\type_integer()",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_integer</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@return Type<int></p>"

                },
                                {
                    name: "type_intersection",
                    caption: "type_intersection",
                    snippet: "\\Flow\\Types\\DSL\\type_intersection(${1:Type $first}, ${2:Type $second}, ${3:Type $types})",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_intersection</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$first</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$second</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$types</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@template T<br>@param Type<T> $first<br>@param Type<T> $second<br>@param Type<T> ...$types<br>@return Type<T></p>"

                },
                                {
                    name: "type_is",
                    caption: "type_is",
                    snippet: "\\Flow\\Types\\DSL\\type_is(${1:Type $type}, ${2:string $typeClass})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_is</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$typeClass</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">bool</span></code></pre><p>@template T<br>@param Type<T> $type<br>@param class-string<Type<mixed>> $typeClass</p>"

                },
                                {
                    name: "type_is_any",
                    caption: "type_is_any",
                    snippet: "\\Flow\\Types\\DSL\\type_is_any(${1:Type $type}, ${2:string $typeClass}, ${3:string $typeClasses})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_is_any</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$typeClass</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$typeClasses</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">bool</span></code></pre><p>@template T<br>@param Type<T> $type<br>@param class-string<Type<mixed>> $typeClass<br>@param class-string<Type<mixed>> ...$typeClasses</p>"

                },
                                {
                    name: "type_is_nullable",
                    caption: "type_is_nullable",
                    snippet: "\\Flow\\Types\\DSL\\type_is_nullable(${1:Type $type})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_is_nullable</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">bool</span></code></pre><p>@template T<br>@param Type<T> $type</p>"

                },
                                {
                    name: "type_json",
                    caption: "type_json",
                    snippet: "\\Flow\\Types\\DSL\\type_json()",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_json</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@return Type<string></p>"

                },
                                {
                    name: "type_list",
                    caption: "type_list",
                    snippet: "\\Flow\\Types\\DSL\\type_list(${1:Type $element})",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_list</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$element</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ListType</span></code></pre><p>@template T<br>@param Type<T> $element<br>@return ListType<T></p>"

                },
                                {
                    name: "type_literal",
                    caption: "type_literal",
                    snippet: "\\Flow\\Types\\DSL\\type_literal(${1:string|int|float|bool $value})",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_literal</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string|int|float|bool</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">LiteralType</span></code></pre><p>@template T of bool|float|int|string<br>@param T $value<br>@return LiteralType<T></p>"

                },
                                {
                    name: "type_map",
                    caption: "type_map",
                    snippet: "\\Flow\\Types\\DSL\\type_map(${1:Type $key_type}, ${2:Type $value_type})",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_map</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$key_type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$value_type</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@template TKey of array-key<br>@template TValue<br>@param Type<TKey> $key_type<br>@param Type<TValue> $value_type<br>@return Type<array<TKey, TValue>></p>"

                },
                                {
                    name: "type_mixed",
                    caption: "type_mixed",
                    snippet: "\\Flow\\Types\\DSL\\type_mixed()",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_mixed</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@return Type<mixed></p>"

                },
                                {
                    name: "type_non_empty_string",
                    caption: "type_non_empty_string",
                    snippet: "\\Flow\\Types\\DSL\\type_non_empty_string()",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_non_empty_string</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@return Type<non-empty-string></p>"

                },
                                {
                    name: "type_null",
                    caption: "type_null",
                    snippet: "\\Flow\\Types\\DSL\\type_null()",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_null</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@return Type<null></p>"

                },
                                {
                    name: "type_numeric_string",
                    caption: "type_numeric_string",
                    snippet: "\\Flow\\Types\\DSL\\type_numeric_string()",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_numeric_string</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@return Type<numeric-string></p>"

                },
                                {
                    name: "type_object",
                    caption: "type_object",
                    snippet: "\\Flow\\Types\\DSL\\type_object()",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_object</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@return Type<object></p>"

                },
                                {
                    name: "type_optional",
                    caption: "type_optional",
                    snippet: "\\Flow\\Types\\DSL\\type_optional(${1:Type $type})",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_optional</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@template T<br>@param Type<T> $type<br>@return Type<T></p>"

                },
                                {
                    name: "type_positive_integer",
                    caption: "type_positive_integer",
                    snippet: "\\Flow\\Types\\DSL\\type_positive_integer()",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_positive_integer</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@return Type<int<0, max>></p>"

                },
                                {
                    name: "type_resource",
                    caption: "type_resource",
                    snippet: "\\Flow\\Types\\DSL\\type_resource()",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_resource</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@return Type<resource></p>"

                },
                                {
                    name: "type_scalar",
                    caption: "type_scalar",
                    snippet: "\\Flow\\Types\\DSL\\type_scalar()",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_scalar</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@return Type<bool|float|int|string></p>"

                },
                                {
                    name: "type_string",
                    caption: "type_string",
                    snippet: "\\Flow\\Types\\DSL\\type_string()",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_string</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@return Type<string></p>"

                },
                                {
                    name: "type_structure",
                    caption: "type_structure",
                    snippet: "\\Flow\\Types\\DSL\\type_structure(${1:array $elements}, ${2:array $optional_elements}, ${3:bool $allow_extra})",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_structure</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$elements</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$optional_elements</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$allow_extra</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@template T<br>@param array<string, Type<T>> $elements<br>@param array<string, Type<T>> $optional_elements<br>@return Type<array<string, T>></p>"

                },
                                {
                    name: "type_time",
                    caption: "type_time",
                    snippet: "\\Flow\\Types\\DSL\\type_time()",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_time</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@return Type<\DateInterval></p>"

                },
                                {
                    name: "type_time_zone",
                    caption: "type_time_zone",
                    snippet: "\\Flow\\Types\\DSL\\type_time_zone()",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_time_zone</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@return Type<\DateTimeZone></p>"

                },
                                {
                    name: "type_union",
                    caption: "type_union",
                    snippet: "\\Flow\\Types\\DSL\\type_union(${1:Type $first}, ${2:Type $second}, ${3:Type $types})",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_union</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$first</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$second</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$types</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@template T<br>@template T<br>@template T<br>@param Type<T> $first<br>@param Type<T> $second<br>@param Type<T> ...$types<br>@return Type<T></p>"

                },
                                {
                    name: "type_uuid",
                    caption: "type_uuid",
                    snippet: "\\Flow\\Types\\DSL\\type_uuid()",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_uuid</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@return Type<Uuid></p>"

                },
                                {
                    name: "type_xml",
                    caption: "type_xml",
                    snippet: "\\Flow\\Types\\DSL\\type_xml()",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_xml</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@return Type<\DOMDocument></p>"

                },
                                {
                    name: "type_xml_element",
                    caption: "type_xml_element",
                    snippet: "\\Flow\\Types\\DSL\\type_xml_element()",
                    meta: "flow-dsl-type",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">type_xml_element</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span></code></pre><p>@return Type<\DOMElement></p>"

                },
                                {
                    name: "ulid",
                    caption: "ulid",
                    snippet: "\\Flow\\ETL\\DSL\\ulid(${1:ScalarFunction|string|null $value})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">ulid</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string|null</span> <span class=\"fn-param\">$value</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Ulid</span></code></pre>"

                },
                                {
                    name: "upper",
                    caption: "upper",
                    snippet: "\\Flow\\ETL\\DSL\\upper(${1:ScalarFunction|string $value})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">upper</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ToUpper</span></code></pre>"

                },
                                {
                    name: "uuid_entry",
                    caption: "uuid_entry",
                    snippet: "\\Flow\\ETL\\DSL\\uuid_entry(${1:string $name}, ${2:Uuid|string|null $value}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-entries",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">uuid_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Uuid|string|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span></code></pre><p>@return Entry<?\Flow\Types\Value\Uuid></p>"

                },
                                {
                    name: "uuid_schema",
                    caption: "uuid_schema",
                    snippet: "\\Flow\\ETL\\DSL\\uuid_schema(${1:string $name}, ${2:bool $nullable}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-schema",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">uuid_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span></code></pre><p>@return Definition<\Flow\Types\Value\Uuid></p>"

                },
                                {
                    name: "uuid_v4",
                    caption: "uuid_v4",
                    snippet: "\\Flow\\ETL\\DSL\\uuid_v4()",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">uuid_v4</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Uuid</span></code></pre>"

                },
                                {
                    name: "uuid_v7",
                    caption: "uuid_v7",
                    snippet: "\\Flow\\ETL\\DSL\\uuid_v7(${1:ScalarFunction|DateTimeInterface|null $value})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">uuid_v7</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|DateTimeInterface|null</span> <span class=\"fn-param\">$value</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Uuid</span></code></pre>"

                },
                                {
                    name: "when",
                    caption: "when",
                    snippet: "\\Flow\\ETL\\DSL\\when(${1:mixed $condition}, ${2:mixed $then}, ${3:mixed $else})",
                    meta: "flow-dsl-scalar-functions",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">when</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$condition</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$then</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$else</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">When</span></code></pre>"

                },
                                {
                    name: "window",
                    caption: "window",
                    snippet: "\\Flow\\ETL\\DSL\\window()",
                    meta: "flow-dsl-data-frame",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">window</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Window</span></code></pre>"

                },
                                {
                    name: "with_entry",
                    caption: "with_entry",
                    snippet: "\\Flow\\ETL\\DSL\\with_entry(${1:string $name}, ${2:ScalarFunction $function})",
                    meta: "flow-dsl-helpers",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">with_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$function</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">WithEntry</span></code></pre>"

                },
                                {
                    name: "write_with_retries",
                    caption: "write_with_retries",
                    snippet: "\\Flow\\ETL\\DSL\\write_with_retries(${1:Loader $loader}, ${2:RetryStrategy $retry_strategy}, ${3:DelayFactory $delay_factory}, ${4:Sleep $sleep})",
                    meta: "flow-dsl-loaders",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">write_with_retries</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Loader</span> <span class=\"fn-param\">$loader</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">RetryStrategy</span> <span class=\"fn-param\">$retry_strategy</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DelayFactory</span> <span class=\"fn-param\">$delay_factory</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Sleep</span> <span class=\"fn-param\">$sleep</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RetryLoader</span></code></pre>"

                },
                                {
                    name: "xml_element_entry",
                    caption: "xml_element_entry",
                    snippet: "\\Flow\\ETL\\DSL\\xml_element_entry(${1:string $name}, ${2:DOMElement|string|null $value}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-entries",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">xml_element_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DOMElement|string|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span></code></pre><p>@return Entry<?\DOMElement></p>"

                },
                                {
                    name: "xml_element_schema",
                    caption: "xml_element_schema",
                    snippet: "\\Flow\\ETL\\DSL\\xml_element_schema(${1:string $name}, ${2:bool $nullable}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-schema",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">xml_element_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span></code></pre><p>@return Definition<\DOMElement></p>"

                },
                                {
                    name: "xml_entry",
                    caption: "xml_entry",
                    snippet: "\\Flow\\ETL\\DSL\\xml_entry(${1:string $name}, ${2:DOMDocument|string|null $value}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-entries",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">xml_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DOMDocument|string|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span></code></pre><p>@return Entry<?\DOMDocument></p>"

                },
                                {
                    name: "xml_schema",
                    caption: "xml_schema",
                    snippet: "\\Flow\\ETL\\DSL\\xml_schema(${1:string $name}, ${2:bool $nullable}, ${3:Metadata $metadata})",
                    meta: "flow-dsl-schema",
                    score: 1_000,
                    docHTML: "<pre><code><span class=\"fn-name\">xml_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-operator\">...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span></code></pre><p>@return Definition<\DOMDocument></p>"

                }
                            ];

            // Filter and score based on prefix match
            var completions = allCompletions.filter(function(item) {
                return !prefix || item.name.toLowerCase().indexOf(prefix.toLowerCase()) === 0;
            }).map(function(item) {
                // Boost exact prefix matches with extremely high score
                if (item.name.toLowerCase().indexOf(prefix.toLowerCase()) === 0) {
                    // Score: 1 million base + bonus for exact match - penalty for length
                    item.score = 1000000 + (10000 - prefix.length);
                }
                return item;
            });

            callback(null, completions);
        },

        getDocTooltip: function(item) {
            if (item.docHTML) {
                return { docHTML: item.docHTML };
            }
        },

        insertMatch: function(editor, data) {
            if (data.snippet) {
                snippetManager.insertSnippet(editor, data.snippet);
            } else {
                editor.completer.insertMatch(data);
            }
        }
    };

    module.exports = flowDslCompleter;
});
